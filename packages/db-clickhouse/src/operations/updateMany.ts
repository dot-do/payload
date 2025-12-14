import type { Document, UpdateMany, UpdateManyArgs } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { combineWhere, QueryBuilder } from '../queries/QueryBuilder.js'
import { generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { extractTitle, parseDataRow } from '../utilities/transform.js'

export const updateMany: UpdateMany = async function updateMany(
  this: ClickHouseAdapter,
  args: UpdateManyArgs,
): Promise<Document[]> {
  const { collection: collectionSlug, data, req, where } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const collection = this.payload.collections[collectionSlug]
  if (!collection) {
    throw new Error(`Collection '${collectionSlug}' not found`)
  }

  const qb = new QueryBuilder()
  const baseWhere = qb.buildBaseWhere(this.namespace, collectionSlug)
  const additionalWhere = qb.buildWhereClause(where as any)
  const whereClause = combineWhere(baseWhere, additionalWhere)
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
    const mergedData = { ...existingData, ...updateData }
    const title = extractTitle(mergedData, titleField, existing.id)

    const insertParams: QueryParams = {
      id: existing.id,
      type: existing.type,
      data: JSON.stringify(mergedData),
      existingCreatedAt: existing.createdAt,
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
        {existingCreatedAt:String},
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
        createdAt: existing.createdAt,
        updatedAt: new Date(now).toISOString(),
      } as Document,
      params: insertParams,
      query: insertQuery,
    }
  })

  // Execute all inserts in parallel
  await Promise.all(
    operations.map((op) =>
      this.client!.command({
        query: op.query,
        query_params: op.params,
      }),
    ),
  )

  return operations.map((op) => op.doc)
}
