import type { UpdateGlobal, UpdateGlobalArgs } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { parseDataRow } from '../utilities/transform.js'

const GLOBALS_TYPE = '_globals'

export const updateGlobal: UpdateGlobal = async function updateGlobal<
  T extends Record<string, unknown> = Record<string, unknown>,
>(this: ClickHouseAdapter, args: UpdateGlobalArgs<T>): Promise<T> {
  const { slug, data, req } = args

  assertValidSlug(slug, 'global')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const findParams: QueryParams = {
    id: slug,
    type: GLOBALS_TYPE,
    ns: this.namespace,
  }

  const findQuery = `
    SELECT *
    FROM ${this.table} FINAL
    WHERE ns = {ns:String}
      AND type = {type:String}
      AND id = {id:String}
      AND deletedAt IS NULL
    LIMIT 1
  `

  const findResult = await this.client.query({
    format: 'JSONEachRow',
    query: findQuery,
    query_params: findParams,
  })

  const existingRows = (await findResult.json())
  const existingRow = existingRows[0]
  const existing = existingRow ? parseDataRow(existingRow) : null

  const now = generateVersion()
  const userId = req?.user?.id ? String(req.user.id) : null
  const { id: _id, createdAt, updatedAt, ...updateData } = data as Record<string, unknown>

  let mergedData: Record<string, unknown>
  let existingCreatedAt: number | string
  let existingCreatedBy: null | string

  if (existing) {
    const existingData = existing.data
    mergedData = { ...existingData, ...updateData }
    existingCreatedAt = existing.createdAt
    existingCreatedBy = existing.createdBy
  } else {
    mergedData = updateData
    existingCreatedAt = now
    existingCreatedBy = userId
  }

  const insertParams: QueryParams = {
    id: slug,
    type: GLOBALS_TYPE,
    data: JSON.stringify(mergedData),
    ns: this.namespace,
    title: slug,
    updatedAt: now,
    v: now,
  }

  // Handle createdAt - either from existing or new timestamp
  if (existing) {
    insertParams.existingCreatedAt = existingCreatedAt
  } else {
    insertParams.createdAt = now
  }

  if (existingCreatedBy) {
    insertParams.createdBy = existingCreatedBy
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
      ${existing ? '{existingCreatedAt:String}' : 'fromUnixTimestamp64Milli({createdAt:Int64})'},
      ${existingCreatedBy ? '{createdBy:String}' : 'NULL'},
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

  const result = {
    id: slug,
    ...mergedData,
    createdAt:
      typeof existingCreatedAt === 'number'
        ? new Date(existingCreatedAt).toISOString()
        : existingCreatedAt,
    updatedAt: new Date(now).toISOString(),
  } as unknown as T

  return result
}
