import type { DeleteVersions, DeleteVersionsArgs } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'
import { generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { parseDataRow, parseDateTime64ToMs } from '../utilities/transform.js'

/**
 * Recursively map 'parent' field to 'id' in where clause.
 * In ClickHouse-native versioning, 'parent' is the document id.
 */
function mapParentToId(where: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {}

  for (const [key, value] of Object.entries(where)) {
    if (key === 'and' && Array.isArray(value)) {
      result.and = value.map((item) =>
        typeof item === 'object' && item !== null
          ? mapParentToId(item as Record<string, unknown>)
          : item,
      )
    } else if (key === 'or' && Array.isArray(value)) {
      result.or = value.map((item) =>
        typeof item === 'object' && item !== null
          ? mapParentToId(item as Record<string, unknown>)
          : item,
      )
    } else if (key === 'parent') {
      result.id = value
    } else {
      result[key] = value
    }
  }

  return result
}

/**
 * Delete versions of documents (soft delete).
 *
 * ClickHouse-native versioning: versions are rows with the same (ns, type, id)
 * but different `v` timestamps. Soft delete by inserting with deletedAt set.
 */
export const deleteVersions: DeleteVersions = async function deleteVersions(
  this: ClickHouseAdapter,
  args: DeleteVersionsArgs,
): Promise<void> {
  const { collection: collectionSlug, req, where } = args

  if (collectionSlug) {
    assertValidSlug(collectionSlug, 'collection')
  }

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)
  const typeParam = qb.addNamedParam('type', collectionSlug)
  const baseWhere = `ns = ${nsParam} AND type = ${typeParam} AND deletedAt IS NULL`

  // Map 'parent' to 'id' in where clause
  const mappedWhere = where ? mapParentToId(where as Record<string, unknown>) : {}
  const additionalWhere = qb.buildWhereClause(mappedWhere as any)
  const whereClause = additionalWhere ? `${baseWhere} AND (${additionalWhere})` : baseWhere
  const findParams = qb.getParams()

  const findQuery = `
    SELECT *
    FROM ${this.table}
    WHERE ${whereClause}
  `

  const findResult = await this.clickhouse.query({
    format: 'JSONEachRow',
    query: findQuery,
    query_params: findParams,
  })

  const existingRows = await findResult.json<DataRow>()

  if (existingRows.length === 0) {
    return
  }

  const now = generateVersion()
  const userId = req?.user?.id ? String(req.user.id) : null

  // Build all soft-delete operations - keep same v to replace the specific version
  const operations = existingRows.map((row) => {
    const existing = parseDataRow(row)
    const existingV = row.v ? parseDateTime64ToMs(row.v) : now

    const deleteParams: QueryParams = {
      id: existing.id,
      type: existing.type,
      createdAtMs: parseDateTime64ToMs(existing.createdAt),
      data: typeof existing.data === 'string' ? existing.data : JSON.stringify(existing.data),
      deletedAt: now,
      ns: existing.ns,
      title: existing.title,
      updatedAtMs: parseDateTime64ToMs(existing.updatedAt),
      v: existingV, // Keep same v to replace this specific version
    }

    if (existing.createdBy) {
      deleteParams.createdBy = existing.createdBy
    }
    if (existing.updatedBy) {
      deleteParams.updatedBy = existing.updatedBy
    }
    if (userId !== null) {
      deleteParams.deletedBy = userId
    }

    const deleteQuery = `
      INSERT INTO ${this.table} (ns, type, id, v, title, data, createdAt, createdBy, updatedAt, updatedBy, deletedAt, deletedBy)
      VALUES (
        {ns:String},
        {type:String},
        {id:String},
        fromUnixTimestamp64Milli({v:Int64}),
        {title:String},
        {data:String},
        fromUnixTimestamp64Milli({createdAtMs:Int64}),
        ${existing.createdBy ? '{createdBy:String}' : 'NULL'},
        fromUnixTimestamp64Milli({updatedAtMs:Int64}),
        ${existing.updatedBy ? '{updatedBy:String}' : 'NULL'},
        fromUnixTimestamp64Milli({deletedAt:Int64}),
        ${userId !== null ? '{deletedBy:String}' : 'NULL'}
      )
    `

    return { params: deleteParams, query: deleteQuery }
  })

  // Execute all soft-deletes in parallel
  await Promise.all(
    operations.map((op) =>
      this.clickhouse!.command({
        query: op.query,
        query_params: op.params,
      }),
    ),
  )
}
