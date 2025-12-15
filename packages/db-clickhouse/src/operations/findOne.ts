import type { FindOne, FindOneArgs, TypeWithID } from 'payload'

import type { ClickHouseAdapter, DataRow } from '../types.js'

import { combineWhere, QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { parseDataRow, rowToDocument } from '../utilities/transform.js'

export const findOne: FindOne = async function findOne<T extends TypeWithID>(
  this: ClickHouseAdapter,
  args: FindOneArgs,
): Promise<null | T> {
  const { collection: collectionSlug, where } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const qb = new QueryBuilder()
  const baseWhereInner = qb.buildBaseWhereNoDeleted(this.namespace, collectionSlug)
  const additionalWhere = qb.buildWhereClause(where as any)
  const innerWhereClause = combineWhere(baseWhereInner, additionalWhere)
  const params = qb.getParams()

  // Use window function to get latest version, filter deletedAt after
  const query = `
    SELECT * EXCEPT(_rn)
    FROM (
      SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ${innerWhereClause}
    )
    WHERE _rn = 1 AND deletedAt IS NULL
    ORDER BY createdAt DESC
    LIMIT 1
  `

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = await result.json<DataRow>()

  if (rows.length === 0) {
    return null
  }

  const parsedRow = parseDataRow(rows[0]!)
  return rowToDocument<T>(parsedRow)
}
