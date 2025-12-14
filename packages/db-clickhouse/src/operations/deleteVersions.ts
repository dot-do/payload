import type { DeleteVersions, DeleteVersionsArgs } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'
import { generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { parseDataRow } from '../utilities/transform.js'

const VERSIONS_TYPE_PREFIX = '_versions_'

export const deleteVersions: DeleteVersions = async function deleteVersions(
  this: ClickHouseAdapter,
  args: DeleteVersionsArgs,
): Promise<void> {
  const { collection: collectionSlug, globalSlug, req, where } = args

  // Validate the slug being used
  if (globalSlug) {
    assertValidSlug(globalSlug, 'global')
  } else if (collectionSlug) {
    assertValidSlug(collectionSlug, 'collection')
  }

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const typePrefix = globalSlug ? '_global_versions_' : VERSIONS_TYPE_PREFIX
  const slug = globalSlug || collectionSlug
  const versionType = `${typePrefix}${slug}`

  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)
  const typeParam = qb.addNamedParam('versionType', versionType)
  const baseWhere = `ns = ${nsParam} AND type = ${typeParam} AND deletedAt IS NULL`

  const additionalWhere = qb.buildWhereClause(where as any)
  const whereClause = additionalWhere ? `${baseWhere} AND (${additionalWhere})` : baseWhere
  const findParams = qb.getParams()

  const findQuery = `
    SELECT *
    FROM ${this.table} FINAL
    WHERE ${whereClause}
  `

  const findResult = await this.client.query({
    format: 'JSONEachRow',
    query: findQuery,
    query_params: findParams,
  })

  const existingRows = (await findResult.json())

  if (existingRows.length === 0) {
    return
  }

  const now = generateVersion()
  const userId = req?.user?.id ? String(req.user.id) : null

  // Build all soft-delete operations
  const operations = existingRows.map((row) => {
    const existing = parseDataRow(row)

    const deleteParams: QueryParams = {
      id: existing.id,
      type: existing.type,
      createdAt: existing.createdAt,
      data: typeof existing.data === 'string' ? existing.data : JSON.stringify(existing.data),
      deletedAt: now,
      ns: existing.ns,
      title: existing.title,
      updatedAt: existing.updatedAt,
      v: now,
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
        {createdAt:String},
        ${existing.createdBy ? '{createdBy:String}' : 'NULL'},
        {updatedAt:String},
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
      this.client!.command({
        query: op.query,
        query_params: op.params,
      }),
    ),
  )
}
