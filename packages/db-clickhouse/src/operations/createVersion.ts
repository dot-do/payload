import type { CreateVersion, CreateVersionArgs, JsonObject, TypeWithVersion } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter } from '../types.js'

import { generateId, generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { extractTitle } from '../utilities/transform.js'

const VERSIONS_TYPE_PREFIX = '_versions_'

export const createVersion: CreateVersion = async function createVersion<
  T extends JsonObject = JsonObject,
>(this: ClickHouseAdapter, args: CreateVersionArgs<T>): Promise<TypeWithVersion<T>> {
  const { autosave, collectionSlug, createdAt, parent, req, updatedAt, versionData } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const collection = this.payload.collections[collectionSlug]
  if (!collection) {
    throw new Error(`Collection '${collectionSlug}' not found`)
  }

  const versionId = generateId(this.idType)
  const now = generateVersion()
  const titleField = collection.config.admin?.useAsTitle
  const title = extractTitle(versionData as Record<string, unknown>, titleField, versionId)
  const userId = req?.user?.id ? String(req.user.id) : null

  // Store parent ID in data - latest is computed dynamically via max(v)
  const versionDoc = {
    ...versionData,
    _autosave: autosave || false,
    _parentId: parent,
  }

  const params: QueryParams = {
    id: versionId,
    type: `${VERSIONS_TYPE_PREFIX}${collectionSlug}`,
    createdAt: now,
    data: JSON.stringify(versionDoc),
    ns: this.namespace,
    title,
    updatedAt: now,
    v: now,
  }

  if (userId !== null) {
    params.createdBy = userId
    params.updatedBy = userId
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
      fromUnixTimestamp64Milli({createdAt:Int64}),
      ${userId !== null ? '{createdBy:String}' : 'NULL'},
      fromUnixTimestamp64Milli({updatedAt:Int64}),
      ${userId !== null ? '{updatedBy:String}' : 'NULL'},
      NULL,
      NULL
    )
  `

  await this.client.command({
    query: insertQuery,
    query_params: params,
  })

  const result: TypeWithVersion<T> = {
    id: versionId,
    createdAt: createdAt || new Date(now).toISOString(),
    parent: typeof parent === 'number' ? parent : parent,
    updatedAt: updatedAt || new Date(now).toISOString(),
    version: versionData,
  }

  return result
}
