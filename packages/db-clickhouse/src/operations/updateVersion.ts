import type { JsonObject, TypeWithVersion, UpdateVersion, UpdateVersionArgs } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { extractTitle, parseDataRow } from '../utilities/transform.js'

const VERSIONS_TYPE_PREFIX = '_versions_'

export const updateVersion: UpdateVersion = async function updateVersion<
  T extends JsonObject = JsonObject,
>(this: ClickHouseAdapter, args: UpdateVersionArgs<T>): Promise<TypeWithVersion<T>> {
  const { collection: collectionSlug, req, versionData } = args
  const id = 'id' in args ? args.id : undefined

  assertValidSlug(collectionSlug, 'collection')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const collection = this.payload.collections[collectionSlug]
  if (!collection) {
    throw new Error(`Collection '${collectionSlug}' not found`)
  }

  const versionType = `${VERSIONS_TYPE_PREFIX}${collectionSlug}`

  const findParams: QueryParams = {
    ns: this.namespace,
    versionType,
  }

  let whereClause = `ns = {ns:String} AND type = {versionType:String} AND deletedAt IS NULL`
  if (id) {
    findParams.id = String(id)
    whereClause += ` AND id = {id:String}`
  }

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
    throw new Error(`Version not found in collection '${collectionSlug}'`)
  }

  const existing = parseDataRow(existingRows[0]!)
  const existingData = existing.data

  const now = generateVersion()
  const titleField = collection.config.admin?.useAsTitle
  const title = extractTitle(
    versionData.version as Record<string, unknown>,
    titleField,
    existing.id,
  )
  const userId = req?.user?.id ? String(req.user.id) : null

  const mergedDoc = {
    ...existingData,
    ...versionData.version,
    _autosave: existingData._autosave,
    _parentId: versionData.parent || existingData._parentId,
  }

  const insertParams: QueryParams = {
    id: existing.id,
    type: existing.type,
    data: JSON.stringify(mergedDoc),
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

  await this.client.command({
    query: insertQuery,
    query_params: insertParams,
  })

  const { _autosave, _parentId, ...cleanVersionData } = mergedDoc

  return {
    id: existing.id,
    createdAt: versionData.createdAt || existing.createdAt,
    parent: _parentId as number | string,
    updatedAt: versionData.updatedAt || new Date(now).toISOString(),
    version: cleanVersionData as T,
  }
}
