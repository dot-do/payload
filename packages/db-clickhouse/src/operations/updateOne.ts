import type { Document, UpdateOne, UpdateOneArgs } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'
import { generateId, generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { deepMerge, extractTitle, parseDataRow, toISOString } from '../utilities/transform.js'

export const updateOne: UpdateOne = async function updateOne(
  this: ClickHouseAdapter,
  args: UpdateOneArgs,
): Promise<Document> {
  const { collection: collectionSlug, data, options, req } = args
  const where = 'where' in args ? args.where : { id: { equals: args.id } }
  const upsert = (options as { upsert?: boolean })?.upsert ?? false

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const collection = this.payload.collections[collectionSlug]
  if (!collection) {
    throw new Error(`Collection '${collectionSlug}' not found`)
  }

  // Build parameterized query to find existing document
  const qb = new QueryBuilder()
  // Data field filters must be applied AFTER window function
  const baseWhereInner = qb.buildBaseWhereNoDeleted(this.namespace, collectionSlug)
  const dataWhere = qb.buildWhereClause(where as any)
  const findParams = qb.getParams()

  // Use window function, filter deletedAt after
  // Apply data filters AFTER window function to ensure we find the latest version
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

  // If no document found and upsert is enabled, create a new one
  if (existingRows.length === 0) {
    if (upsert) {
      return this.create({
        collection: collectionSlug,
        data,
        req,
      })
    }
    throw new Error(`Document not found in collection '${collectionSlug}'`)
  }

  const existing = parseDataRow(existingRows[0]!)
  const existingData = existing.data
  const hasTimestamps = collection.config.timestamps !== false

  const { id: _id, createdAt: userCreatedAt, updatedAt: userUpdatedAt, ...updateData } = data
  const mergedData = deepMerge(existingData, updateData)

  const now = generateVersion()
  const titleField = collection.config.admin?.useAsTitle
  const title = extractTitle(mergedData, titleField, existing.id)
  const userId = req?.user?.id ? String(req.user.id) : null

  // Respect user-provided timestamps
  // If updatedAt is explicitly null, keep the existing value
  let updatedAtValue: number | string
  if (userUpdatedAt === null) {
    // Convert ClickHouse format to ISO format
    updatedAtValue = toISOString(existing.updatedAt) || new Date(now).toISOString()
  } else if (userUpdatedAt !== undefined) {
    updatedAtValue = new Date(userUpdatedAt as string).getTime()
  } else {
    updatedAtValue = now
  }

  // If createdAt is provided, use it; otherwise keep existing
  // Store both ISO string (for result) and milliseconds (for ClickHouse)
  // Convert ClickHouse format to ISO format for existing timestamps
  const createdAtIso = userCreatedAt
    ? new Date(userCreatedAt as string).toISOString()
    : toISOString(existing.createdAt) || new Date(now).toISOString()
  const createdAtMs = new Date(createdAtIso).getTime()

  // Build insert params for the new version
  const insertParams: QueryParams = {
    id: existing.id,
    type: existing.type,
    createdAtMs,
    data: JSON.stringify(mergedData),
    ns: existing.ns,
    title,
    updatedAt:
      typeof updatedAtValue === 'number' ? updatedAtValue : new Date(updatedAtValue).getTime(),
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

  await this.clickhouse.command({
    query: insertQuery,
    query_params: insertParams,
  })

  const result: Document = {
    id: existing.id,
    ...mergedData,
  }

  // Only add timestamps if the collection has timestamps enabled
  if (hasTimestamps) {
    result.createdAt = createdAtIso
    result.updatedAt =
      typeof updatedAtValue === 'number' ? new Date(updatedAtValue).toISOString() : updatedAtValue
  }

  return result
}
