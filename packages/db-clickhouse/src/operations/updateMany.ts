import type { Document, UpdateMany, UpdateManyArgs } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { combineWhere, QueryBuilder } from '../queries/QueryBuilder.js'
import { generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import {
  deepMerge,
  extractTitle,
  parseDataRow,
  parseDateTime64ToMs,
  toISOString,
} from '../utilities/transform.js'

export const updateMany: UpdateMany = async function updateMany(
  this: ClickHouseAdapter,
  args: UpdateManyArgs,
): Promise<Document[]> {
  const { collection: collectionSlug, data, limit, req, where } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const collection = this.payload.collections[collectionSlug]
  if (!collection) {
    throw new Error(`Collection '${collectionSlug}' not found`)
  }

  const qb = new QueryBuilder()
  const baseWhereInner = qb.buildBaseWhereNoDeleted(this.namespace, collectionSlug)
  const additionalWhere = qb.buildWhereClause(where as any)
  const innerWhereClause = combineWhere(baseWhereInner, additionalWhere)
  const findParams = qb.getParams()

  // Use window function, filter deletedAt after
  // Apply limit if specified
  const limitClause = limit && limit > 0 ? `LIMIT ${limit}` : ''
  const findQuery = `
    SELECT * EXCEPT(_rn)
    FROM (
      SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ${innerWhereClause}
    )
    WHERE _rn = 1 AND deletedAt IS NULL
    ${limitClause}
  `

  const findResult = await this.clickhouse.query({
    format: 'JSONEachRow',
    query: findQuery,
    query_params: findParams,
  })

  const existingRows = await findResult.json<DataRow>()

  if (existingRows.length === 0) {
    return []
  }

  const now = generateVersion()
  const titleField = collection.config.admin?.useAsTitle
  const userId = req?.user?.id ? String(req.user.id) : null
  const { id: _id, createdAt, updatedAt, ...updateData } = data

  // Build all insert operations
  const operations = existingRows.map((row) => {
    const existing = parseDataRow(row)
    const existingData = existing.data
    const mergedData = deepMerge(existingData, updateData)
    const title = extractTitle(mergedData, titleField, existing.id)

    const createdAtMs = parseDateTime64ToMs(existing.createdAt)

    const insertParams: QueryParams = {
      id: existing.id,
      type: existing.type,
      createdAtMs,
      data: JSON.stringify(mergedData),
      ns: existing.ns,
      title,
      updatedAt: now,
      v: now,
    }

    if (existing.createdBy) {
      insertParams.createdBy = existing.createdBy
    }
    if (userId !== null) {
      insertParams.updatedBy = userId
    }

    const insertQuery = `
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
        fromUnixTimestamp64Milli({updatedAt:Int64}),
        ${userId !== null ? '{updatedBy:String}' : 'NULL'},
        NULL,
        NULL
      )
    `

    return {
      doc: {
        id: existing.id,
        ...mergedData,
        createdAt: toISOString(existing.createdAt),
        updatedAt: new Date(now).toISOString(),
      } as Document,
      params: insertParams,
      query: insertQuery,
    }
  })

  // Execute all inserts in parallel
  await Promise.all(
    operations.map((op) =>
      this.clickhouse!.command({
        query: op.query,
        query_params: op.params,
      }),
    ),
  )

  return operations.map((op) => op.doc)
}
