import type { CreateVersion, CreateVersionArgs, JsonObject, TypeWithVersion } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter } from '../types.js'

import { generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { extractTitle } from '../utilities/transform.js'

/**
 * Get the versions collection type name.
 * Payload stores versions with the naming convention: _${collection}_versions
 */
function getVersionsType(collectionSlug: string): string {
  return `_${collectionSlug}_versions`
}

/**
 * Create a new version of a document.
 *
 * Versions are stored with type = `_${collection}_versions` to match Payload's versioning convention.
 * Each version gets a unique `v` timestamp that serves as its identifier.
 */
export const createVersion: CreateVersion = async function createVersion<
  T extends JsonObject = JsonObject,
>(this: ClickHouseAdapter, args: CreateVersionArgs<T>): Promise<TypeWithVersion<T>> {
  const { autosave, collectionSlug, createdAt, parent, req, updatedAt, versionData } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const collection = this.payload.collections[collectionSlug]
  if (!collection) {
    throw new Error(`Collection '${collectionSlug}' not found`)
  }

  const now = generateVersion()
  const titleField = collection.config.admin?.useAsTitle
  const title = extractTitle(versionData as Record<string, unknown>, titleField, String(parent))
  const userId = req?.user?.id ? String(req.user.id) : null

  // Build version document with parent reference and optional autosave flag
  const versionDoc = {
    parent: String(parent),
    version: versionData,
    ...(autosave && { _autosave: true }),
  }

  // Versions are stored with type = _${collection}_versions
  const versionType = getVersionsType(collectionSlug)
  const params: QueryParams = {
    id: String(now), // Version ID is the v timestamp
    type: versionType,
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

  await this.clickhouse.command({
    query: insertQuery,
    query_params: params,
  })

  // Return version with v timestamp as the version id
  const result: TypeWithVersion<T> = {
    id: String(now),
    createdAt: createdAt || new Date(now).toISOString(),
    parent,
    updatedAt: updatedAt || new Date(now).toISOString(),
    version: versionData,
  }

  return result
}
