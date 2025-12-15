import type { UpdateGlobal, UpdateGlobalArgs } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { deepMerge, parseDataRow, parseDateTime64ToMs } from '../utilities/transform.js'

const GLOBALS_TYPE = '_globals'

export const updateGlobal: UpdateGlobal = async function updateGlobal<
  T extends Record<string, unknown> = Record<string, unknown>,
>(this: ClickHouseAdapter, args: UpdateGlobalArgs<T>): Promise<T> {
  const { slug, data, req } = args

  assertValidSlug(slug, 'global')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const findParams: QueryParams = {
    id: slug,
    type: GLOBALS_TYPE,
    ns: this.namespace,
  }

  // Use window function, filter deletedAt after
  const findQuery = `
    SELECT * EXCEPT(_rn)
    FROM (
      SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ns = {ns:String}
        AND type = {type:String}
        AND id = {id:String}
    )
    WHERE _rn = 1 AND deletedAt IS NULL
    LIMIT 1
  `

  const findResult = await this.clickhouse.query({
    format: 'JSONEachRow',
    query: findQuery,
    query_params: findParams,
  })

  const existingRows = await findResult.json<DataRow>()
  const existingRow = existingRows[0]
  const existing = existingRow ? parseDataRow(existingRow) : null

  const now = generateVersion()
  const userId = req?.user?.id ? String(req.user.id) : null
  const { id: _id, createdAt, updatedAt, ...updateData } = data as Record<string, unknown>

  let mergedData: Record<string, unknown>
  let createdAtMs: number
  let existingCreatedBy: null | string

  if (existing) {
    const existingData = existing.data
    mergedData = deepMerge(existingData, updateData)
    createdAtMs = parseDateTime64ToMs(existing.createdAt)
    existingCreatedBy = existing.createdBy
  } else {
    mergedData = updateData
    createdAtMs = now
    existingCreatedBy = userId
  }

  const insertParams: QueryParams = {
    id: slug,
    type: GLOBALS_TYPE,
    createdAtMs,
    data: JSON.stringify(mergedData),
    ns: this.namespace,
    title: slug,
    updatedAt: now,
    v: now,
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
      fromUnixTimestamp64Milli({createdAtMs:Int64}),
      ${existingCreatedBy ? '{createdBy:String}' : 'NULL'},
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

  const result = {
    id: slug,
    ...mergedData,
    createdAt: new Date(createdAtMs).toISOString(),
    globalType: slug,
    updatedAt: new Date(now).toISOString(),
  } as unknown as T

  return result
}
