import type { FindOne, FindOneArgs, TypeWithID } from 'payload'

import type { ClickHouseAdapter, DataRow } from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { hasCustomNumericID, parseDataRow, rowToDocument } from '../utilities/transform.js'

export const findOne: FindOne = async function findOne<T extends TypeWithID>(
  this: ClickHouseAdapter,
  args: FindOneArgs,
): Promise<null | T> {
  const { collection: collectionSlug, where } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const collection = this.payload.collections[collectionSlug]
  const numericID = collection ? hasCustomNumericID(collection.config.fields) : false

  const qb = new QueryBuilder()
  // Build base WHERE for inner query (only ns, type)
  // Data field filters must be applied AFTER window function
  const baseWhereInner = qb.buildBaseWhereNoDeleted(this.namespace, collectionSlug)
  const dataWhere = qb.buildWhereClause(where as any)
  const params = qb.getParams()

  // Use window function to get latest version, filter deletedAt after
  // Apply data filters AFTER window function to ensure we filter on latest version
  const outerWhere = dataWhere
    ? `_rn = 1 AND deletedAt IS NULL AND (${dataWhere})`
    : '_rn = 1 AND deletedAt IS NULL'
  const query = `
    SELECT * EXCEPT(_rn)
    FROM (
      SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ${baseWhereInner}
    )
    WHERE ${outerWhere}
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
  return rowToDocument<T>(parsedRow, numericID)
}
