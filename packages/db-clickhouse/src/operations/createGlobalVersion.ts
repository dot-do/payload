import type {
  CreateGlobalVersion,
  CreateGlobalVersionArgs,
  JsonObject,
  TypeWithVersion,
} from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter } from '../types.js'

import { generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'

const GLOBAL_TYPE_PREFIX = '_global_'

/**
 * Create a new version of a global.
 *
 * ClickHouse-native versioning: versions are stored as rows with the same (ns, type, id)
 * but different `v` timestamps. For globals, id is the global slug.
 */
export const createGlobalVersion: CreateGlobalVersion = async function createGlobalVersion<
  T extends JsonObject = JsonObject,
>(
  this: ClickHouseAdapter,
  args: CreateGlobalVersionArgs<T>,
): Promise<Omit<TypeWithVersion<T>, 'parent'>> {
  const { autosave, createdAt, globalSlug, req, updatedAt, versionData } = args

  assertValidSlug(globalSlug, 'global')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const now = generateVersion()
  const userId = req?.user?.id ? String(req.user.id) : null

  // Store autosave flag in data if needed
  const versionDoc = autosave ? { ...versionData, _autosave: true } : versionData

  // For globals, id is the global slug (there's only one instance per global)
  const params: QueryParams = {
    id: globalSlug,
    type: `${GLOBAL_TYPE_PREFIX}${globalSlug}`,
    createdAt: now,
    data: JSON.stringify(versionDoc),
    ns: this.namespace,
    title: globalSlug,
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
  const result: Omit<TypeWithVersion<T>, 'parent'> = {
    id: String(now),
    createdAt: createdAt || new Date(now).toISOString(),
    updatedAt: updatedAt || new Date(now).toISOString(),
    version: versionData,
  }

  return result
}
