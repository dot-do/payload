import type { DeleteOne, DeleteOneArgs, Document } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { combineWhere, QueryBuilder } from '../queries/QueryBuilder.js'
import { generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { parseDataRow, rowToDocument } from '../utilities/transform.js'

export const deleteOne: DeleteOne = async function deleteOne(
  this: ClickHouseAdapter,
  args: DeleteOneArgs,
): Promise<Document> {
  const { collection: collectionSlug, req, where } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
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
    LIMIT 1
  `

  const findResult = await this.client.query({
    format: 'JSONEachRow',
    query: findQuery,
    query_params: findParams,
  })

  const existingRows = (await findResult.json())

  if (existingRows.length === 0) {
    throw new Error(`Document not found in collection '${collectionSlug}'`)
  }

  const existing = parseDataRow(existingRows[0]!)
  const now = generateVersion()
  const userId = req?.user?.id ? String(req.user.id) : null

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

  await this.client.command({
    query: deleteQuery,
    query_params: deleteParams,
  })

  return rowToDocument<Document>(existing)
}
