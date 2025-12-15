import type { FindGlobal, FindGlobalArgs } from 'payload'

import type { ClickHouseAdapter, DataRow } from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { parseDataRow, rowToDocument } from '../utilities/transform.js'

const GLOBALS_TYPE = '_globals'

export const findGlobal: FindGlobal = async function findGlobal<
  T extends Record<string, unknown> = Record<string, unknown>,
>(this: ClickHouseAdapter, args: FindGlobalArgs): Promise<T> {
  const { slug, where } = args

  assertValidSlug(slug, 'global')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const qb = new QueryBuilder()

  // Build base WHERE for this specific global
  qb.addNamedParam('ns', this.namespace)
  qb.addNamedParam('type', GLOBALS_TYPE)
  qb.addNamedParam('id', slug)

  // Add access control conditions if provided
  const accessControlWhere = qb.buildWhereClause(where)

  const params = qb.getParams()

  // Use window function to get latest version, then filter by access control and deletedAt
  const query = `
    SELECT * EXCEPT(_rn)
    FROM (
      SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ns = {ns:String}
        AND type = {type:String}
        AND id = {id:String}
    )
    WHERE _rn = 1 AND deletedAt IS NULL${accessControlWhere ? ` AND (${accessControlWhere})` : ''}
    LIMIT 1
  `

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = await result.json<DataRow>()

  if (rows.length === 0) {
    // No document found - either doesn't exist or access denied
    // Return empty object if there's a where clause (access control denied)
    // Otherwise return base object (global doesn't exist yet)
    if (where && Object.keys(where).length > 0) {
      return {} as T
    }
    return { id: slug, globalType: slug } as unknown as T
  }

  const parsedRow = parseDataRow(rows[0]!)
  const doc = rowToDocument<{ id: string } & T>(parsedRow)

  return { ...doc, globalType: slug } as unknown as T
}
