import type { Create, CreateArgs, Document } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter } from '../types.js'

import { generateId, generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { extractTitle } from '../utilities/transform.js'

export const create: Create = async function create(
  this: ClickHouseAdapter,
  args: CreateArgs,
): Promise<Document> {
  const { collection: collectionSlug, data, req } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const collection = this.payload.collections[collectionSlug]
  if (!collection) {
    throw new Error(`Collection '${collectionSlug}' not found`)
  }

  const id = data.id ? String(data.id) : generateId(this.idType)
  const now = generateVersion()
  const titleField = collection.config.admin?.useAsTitle
  const title = extractTitle(data, titleField, id)
  const userId = req?.user?.id ? String(req.user.id) : null

  const { id: _id, createdAt, updatedAt, ...docData } = data

  const params: QueryParams = {
    id,
    type: collectionSlug,
    createdAt: now,
    data: JSON.stringify(docData),
    ns: this.namespace,
    title,
    updatedAt: now,
    v: now,
  }

  // Handle nullable fields
  if (userId !== null) {
    params.createdBy = userId
    params.updatedBy = userId
  }

  const query = `
    INSERT INTO ${this.table} (ns, type, id, v, title, data, createdAt, createdBy, updatedAt, updatedBy, deletedAt, deletedBy)
    VALUES (
      {ns:String},
      {type:String},
      {id:String},
      fromUnixTimestamp64Milli({v:Int64}),
      {title:String},
      {data:String},
      fromUnixTimestamp64Milli({createdAt:Int64}),
      ${userId !== null ? '{createdBy:String}' : 'NULL'},
      fromUnixTimestamp64Milli({updatedAt:Int64}),
      ${userId !== null ? '{updatedBy:String}' : 'NULL'},
      NULL,
      NULL
    )
  `

  await this.client.command({
    query,
    query_params: params,
  })

  const result: Document = {
    id,
    ...docData,
    createdAt: new Date(now).toISOString(),
    updatedAt: new Date(now).toISOString(),
  }

  return result
}
