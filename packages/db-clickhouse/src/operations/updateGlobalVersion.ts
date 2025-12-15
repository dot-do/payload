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
import { parseDataRow, parseDateTime64ToMs, toISOString } from '../utilities/transform.js'

const GLOBAL_TYPE_PREFIX = '_global_'

/**
 * Update a version of a global.
 *
 * ClickHouse-native versioning: the version id IS the `v` timestamp.
 * We find the row by (ns, type, id, v) and insert a new row with updated data.
 */
export const updateGlobalVersion: UpdateGlobalVersion = async function updateGlobalVersion<
  T extends JsonObject = JsonObject,
>(this: ClickHouseAdapter, args: UpdateGlobalVersionArgs<T>): Promise<TypeWithVersion<T>> {
  const { global: globalSlug, req, versionData } = args
  // In ClickHouse-native versioning, the version "id" is the v timestamp
  const versionId = 'id' in args ? args.id : undefined

  assertValidSlug(globalSlug, 'global')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const globalType = `${GLOBAL_TYPE_PREFIX}${globalSlug}`

  // Parse the version id to get the v timestamp
  const vTimestamp = versionId ? parseInt(String(versionId), 10) : undefined

  const findParams: QueryParams = {
    type: globalType,
    globalId: globalSlug,
    ns: this.namespace,
  }

  let whereClause = `ns = {ns:String} AND type = {type:String} AND id = {globalId:String} AND deletedAt IS NULL`

  // Filter by v timestamp if provided
  if (vTimestamp && !isNaN(vTimestamp)) {
    findParams.vTimestamp = vTimestamp
    whereClause += ` AND v = fromUnixTimestamp64Milli({vTimestamp:Int64})`
  }

  const findQuery = `
    SELECT *
    FROM ${this.table}
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
    throw new Error(`Global version not found for '${globalSlug}'`)
  }

  const existing = parseDataRow(existingRows[0]!)
  const existingData = existing.data

  const now = generateVersion()
  const userId = req?.user?.id ? String(req.user.id) : null

  // Merge existing data with new version content
  const mergedDoc = {
    ...existingData,
    ...versionData.version,
    _autosave: existingData._autosave,
  }

  // Insert updated version with same v (to replace the existing row)
  const existingV = existingRows[0]!.v ? parseDateTime64ToMs(existingRows[0]!.v) : now

  const insertParams: QueryParams = {
    id: globalSlug,
    type: globalType,
    createdAtMs: parseDateTime64ToMs(existing.createdAt),
    data: JSON.stringify(mergedDoc),
    ns: existing.ns,
    title: globalSlug,
    updatedAt: now,
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

  const { _autosave, ...cleanVersionData } = mergedDoc

  return {
    id: String(existingV),
    createdAt: versionData.createdAt
      ? new Date(versionData.createdAt).toISOString()
      : toISOString(existing.createdAt) || new Date(now).toISOString(),
    parent: globalSlug,
    updatedAt: versionData.updatedAt
      ? new Date(versionData.updatedAt).toISOString()
      : new Date(now).toISOString(),
    version: cleanVersionData as T,
  }
}
