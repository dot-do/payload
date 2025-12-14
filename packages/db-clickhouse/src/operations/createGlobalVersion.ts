import type {
  CreateGlobalVersion,
  CreateGlobalVersionArgs,
  JsonObject,
  TypeWithVersion,
} from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter } from '../types.js'

import { generateId, generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'

const GLOBAL_VERSIONS_TYPE_PREFIX = '_global_versions_'

export const createGlobalVersion: CreateGlobalVersion = async function createGlobalVersion<
  T extends JsonObject = JsonObject,
>(
  this: ClickHouseAdapter,
  args: CreateGlobalVersionArgs<T>,
): Promise<Omit<TypeWithVersion<T>, 'parent'>> {
  const { autosave, createdAt, globalSlug, req, updatedAt, versionData } = args

  assertValidSlug(globalSlug, 'global')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const versionId = generateId(this.idType)
  const now = generateVersion()
  const userId = req?.user?.id ? String(req.user.id) : null

  const versionDoc = {
    ...versionData,
    _autosave: autosave || false,
    _globalSlug: globalSlug,
  }

  const params: QueryParams = {
    id: versionId,
    type: `${GLOBAL_VERSIONS_TYPE_PREFIX}${globalSlug}`,
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

  await this.client.command({
    query: insertQuery,
    query_params: params,
  })

  const result: Omit<TypeWithVersion<T>, 'parent'> = {
    id: versionId,
    createdAt: createdAt || new Date(now).toISOString(),
    updatedAt: updatedAt || new Date(now).toISOString(),
    version: versionData,
  }

  return result
}
