import type {
  JsonObject,
  TypeWithVersion,
  UpdateGlobalVersion,
  UpdateGlobalVersionArgs,
} from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { parseDataRow } from '../utilities/transform.js'

const GLOBAL_VERSIONS_TYPE_PREFIX = '_global_versions_'

export const updateGlobalVersion: UpdateGlobalVersion = async function updateGlobalVersion<
  T extends JsonObject = JsonObject,
>(this: ClickHouseAdapter, args: UpdateGlobalVersionArgs<T>): Promise<TypeWithVersion<T>> {
  const { global: globalSlug, req, versionData } = args
  const id = 'id' in args ? args.id : undefined

  assertValidSlug(globalSlug, 'global')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const versionType = `${GLOBAL_VERSIONS_TYPE_PREFIX}${globalSlug}`

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
    throw new Error(`Global version not found for '${globalSlug}'`)
  }

  const existing = parseDataRow(existingRows[0]!)
  const existingData = existing.data

  const now = generateVersion()
  const userId = req?.user?.id ? String(req.user.id) : null

  const mergedDoc = {
    ...existingData,
    ...versionData.version,
    _autosave: existingData._autosave,
    _globalSlug: existingData._globalSlug,
  }

  const insertParams: QueryParams = {
    id: existing.id,
    type: existing.type,
    data: JSON.stringify(mergedDoc),
    existingCreatedAt: existing.createdAt,
    ns: existing.ns,
    title: existing.title,
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

  const { _autosave, _globalSlug, ...cleanVersionData } = mergedDoc

  return {
    id: existing.id,
    createdAt: versionData.createdAt || existing.createdAt,
    parent: globalSlug,
    updatedAt: versionData.updatedAt || new Date(now).toISOString(),
    version: cleanVersionData as T,
  }
}
