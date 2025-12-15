import type { JsonObject, TypeWithVersion, UpdateVersion, UpdateVersionArgs } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { extractTitle, parseDataRow, parseDateTime64ToMs } from '../utilities/transform.js'

/**
 * Get the versions collection type name.
 * Payload stores versions with the naming convention: _${collection}_versions
 */
function getVersionsType(collectionSlug: string): string {
  return `_${collectionSlug}_versions`
}

/**
 * Update a version of a document.
 *
 * Versions are stored with type = `_${collection}_versions`.
 * The version id is the row id (v timestamp as string).
 */
export const updateVersion: UpdateVersion = async function updateVersion<
  T extends JsonObject = JsonObject,
>(this: ClickHouseAdapter, args: UpdateVersionArgs<T>): Promise<TypeWithVersion<T>> {
  const { collection: collectionSlug, req, versionData } = args
  // Version ID is the row id (v timestamp as string)
  const versionId = 'id' in args ? args.id : undefined

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const collection = this.payload.collections[collectionSlug]
  if (!collection) {
    throw new Error(`Collection '${collectionSlug}' not found`)
  }

  // Use versions type: _${collection}_versions
  const versionsType = getVersionsType(collectionSlug)

  const findParams: QueryParams = {
    type: versionsType,
    ns: this.namespace,
  }

  let whereClause = `ns = {ns:String} AND type = {type:String} AND deletedAt IS NULL`

  // Filter by version id if provided
  if (versionId) {
    findParams.versionId = String(versionId)
    whereClause += ` AND id = {versionId:String}`
  }

  const findQuery = `
    SELECT *
    FROM ${this.table} FINAL
    WHERE ${whereClause}
    LIMIT 1
  `

  const findResult = await this.clickhouse.query({
    format: 'JSONEachRow',
    query: findQuery,
    query_params: findParams,
  })

  const existingRows = await findResult.json<DataRow>()

  if (existingRows.length === 0) {
    throw new Error(`Version not found in collection '${collectionSlug}'`)
  }

  const existing = parseDataRow(existingRows[0]!)
  const existingData = existing.data as {
    _autosave?: boolean
    parent: string
    version: Record<string, unknown>
  }

  const now = generateVersion()
  const titleField = collection.config.admin?.useAsTitle
  const userId = req?.user?.id ? String(req.user.id) : null

  // Merge existing version content with new version content
  const mergedVersionContent = {
    ...existingData.version,
    ...(versionData as Record<string, unknown>),
  }
  // Remove internal fields that shouldn't be in version content
  delete (mergedVersionContent as any).createdAt
  delete (mergedVersionContent as any).updatedAt

  const title = extractTitle(mergedVersionContent, titleField, existingData.parent)

  // Build updated version document
  const updatedVersionDoc = {
    parent: existingData.parent,
    version: mergedVersionContent,
    ...(existingData._autosave && { _autosave: true }),
  }

  // Keep the same v (timestamp) to update in place via ReplacingMergeTree
  const existingV = existingRows[0]!.v ? parseDateTime64ToMs(existingRows[0]!.v) : now

  // Respect user-provided timestamps in the INSERT
  const createdAtMs = versionData.createdAt
    ? new Date(versionData.createdAt).getTime()
    : parseDateTime64ToMs(existing.createdAt)
  const updatedAtMs = versionData.updatedAt
    ? new Date(versionData.updatedAt).getTime()
    : now

  const insertParams: QueryParams = {
    id: existing.id, // Keep the same version id
    type: versionsType,
    createdAtMs,
    data: JSON.stringify(updatedVersionDoc),
    ns: existing.ns,
    title,
    updatedAt: updatedAtMs,
    v: existingV, // Keep the same v to update in place
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

  return {
    id: existing.id, // Version ID
    createdAt: new Date(createdAtMs).toISOString(),
    parent: existingData.parent,
    updatedAt: new Date(updatedAtMs).toISOString(),
    version: mergedVersionContent as T,
  }
}
