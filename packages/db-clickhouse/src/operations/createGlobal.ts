import type { CreateGlobal, CreateGlobalArgs } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter } from '../types.js'

import { generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'

const GLOBALS_TYPE = '_globals'

export const createGlobal: CreateGlobal = async function createGlobal<
  T extends Record<string, unknown> = Record<string, unknown>,
>(this: ClickHouseAdapter, args: CreateGlobalArgs<T>): Promise<T> {
  const { slug, data, req } = args

  assertValidSlug(slug, 'global')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const now = generateVersion()
  const userId = req?.user?.id ? String(req.user.id) : null

  const { id: _id, createdAt, updatedAt, ...docData } = data as Record<string, unknown>

  const params: QueryParams = {
    id: slug,
    type: GLOBALS_TYPE,
    createdAt: now,
    data: JSON.stringify(docData),
    ns: this.namespace,
    title: slug,
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

  const result = {
    id: slug,
    ...docData,
    createdAt: new Date(now).toISOString(),
    updatedAt: new Date(now).toISOString(),
  } as unknown as T

  return result
}
