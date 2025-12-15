import type { DeleteOne, DeleteOneArgs, Document } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'
import { generateVersion } from '../utilities/generateId.js'
import { softDeleteRelationships } from '../utilities/relationships.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import {
  hasCustomNumericID,
  parseDataRow,
  parseDateTime64ToMs,
  rowToDocument,
} from '../utilities/transform.js'

export const deleteOne: DeleteOne = async function deleteOne(
  this: ClickHouseAdapter,
  args: DeleteOneArgs,
): Promise<Document> {
  const { collection: collectionSlug, req, where } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const collection = this.payload.collections[collectionSlug]
  const numericID = collection ? hasCustomNumericID(collection.config.fields) : false

  const qb = new QueryBuilder()
  // Build base WHERE for inner query (only ns, type)
  // Data field filters must be applied AFTER window function
  const baseWhereInner = qb.buildBaseWhereNoDeleted(this.namespace, collectionSlug)
  const dataWhere = qb.buildWhereClause(where as any)
  const findParams = qb.getParams()

  // Use window function, filter deletedAt after
  // Apply data filters AFTER window function to ensure we filter on latest version
  const outerWhere = dataWhere
    ? `_rn = 1 AND deletedAt IS NULL AND (${dataWhere})`
    : '_rn = 1 AND deletedAt IS NULL'
  const findQuery = `
    SELECT * EXCEPT(_rn)
    FROM (
      SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ${baseWhereInner}
    )
    WHERE ${outerWhere}
    LIMIT 1
  `

  const findResult = await this.clickhouse.query({
    format: 'JSONEachRow',
    query: findQuery,
    query_params: findParams,
  })

  const existingRows = await findResult.json<DataRow>()

  if (existingRows.length === 0) {
    throw new Error(`Document not found in collection '${collectionSlug}'`)
  }

  const row = existingRows[0]!
  const existing = parseDataRow(row)
  const now = generateVersion()
  const userId = req?.user?.id ? String(req.user.id) : null

  // Ensure soft-delete v is always greater than the existing doc's v
  // This guarantees the soft-delete row wins in ORDER BY v DESC
  const existingV = row.v ? parseDateTime64ToMs(row.v) : 0
  const deleteV = Math.max(now, existingV + 1)

  // For soft delete, set data to {} to minimize storage
  const deleteParams: QueryParams = {
    id: existing.id,
    type: existing.type,
    createdAtMs: parseDateTime64ToMs(existing.createdAt),
    data: '{}',
    deletedAt: deleteV,
    ns: existing.ns,
    title: existing.title || '',
    updatedAtMs: parseDateTime64ToMs(existing.updatedAt),
    v: deleteV,
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

  await this.clickhouse.command({
    query: deleteQuery,
    query_params: deleteParams,
  })

  // Soft-delete all relationships for this document
  await softDeleteRelationships(this.clickhouse, this.table, {
    fromId: existing.id,
    fromType: collectionSlug,
    ns: this.namespace,
    v: deleteV,
  })

  // Note: ClickHouse provides eventual consistency via ReplacingMergeTree.
  // The soft-delete row will be merged asynchronously. For immediate consistency
  // in tests, use PAYLOAD_DROP_DATABASE=true which clears data at connect time.

  return rowToDocument<Document>(existing, numericID)
}
