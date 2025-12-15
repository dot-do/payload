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
  const { collection: collectionSlug, data, req, returning = true } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
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
  const hasTimestamps = collection.config.timestamps !== false

  const { id: _id, createdAt: userCreatedAt, updatedAt: userUpdatedAt, ...docData } = data

  // Respect user-provided timestamps, or use current time
  const createdAtValue = userCreatedAt ? new Date(userCreatedAt as string).getTime() : now
  const updatedAtValue = userUpdatedAt ? new Date(userUpdatedAt as string).getTime() : now

  const params: QueryParams = {
    id,
    type: collectionSlug,
    createdAt: hasTimestamps ? createdAtValue : now,
    data: JSON.stringify(docData),
    ns: this.namespace,
    title,
    updatedAt: hasTimestamps ? updatedAtValue : now,
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

  await this.clickhouse.command({
    query,
    query_params: params,
  })

  // If returning is false, return null to skip building the response document
  if (!returning) {
    return null as unknown as Document
  }

  const result: Document = {
    id,
    ...docData,
  }

  // Only add timestamps if the collection has timestamps enabled
  if (hasTimestamps) {
    result.createdAt = new Date(createdAtValue).toISOString()
    result.updatedAt = new Date(updatedAtValue).toISOString()
  }

  return result
}
