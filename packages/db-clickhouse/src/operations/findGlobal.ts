import type { FindGlobal, FindGlobalArgs } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { assertValidSlug } from '../utilities/sanitize.js'
import { parseDataRow, rowToDocument } from '../utilities/transform.js'

const GLOBALS_TYPE = '_globals'

export const findGlobal: FindGlobal = async function findGlobal<
  T extends Record<string, unknown> = Record<string, unknown>,
>(this: ClickHouseAdapter, args: FindGlobalArgs): Promise<T> {
  const { slug } = args

  assertValidSlug(slug, 'global')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const params: QueryParams = {
    id: slug,
    type: GLOBALS_TYPE,
    ns: this.namespace,
  }

  const query = `
    SELECT *
    FROM ${this.table} FINAL
    WHERE ns = {ns:String}
      AND type = {type:String}
      AND id = {id:String}
      AND deletedAt IS NULL
    LIMIT 1
  `

  const result = await this.client.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = (await result.json())

  if (rows.length === 0) {
    return { id: slug } as unknown as T
  }

  const parsedRow = parseDataRow(rows[0]!)
  return rowToDocument<{ id: string } & T>(parsedRow) as unknown as T
}
