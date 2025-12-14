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

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const qb = new QueryBuilder()
  const baseWhere = qb.buildBaseWhere(this.namespace, collectionSlug)
  const additionalWhere = qb.buildWhereClause(where as any)
  const whereClause = combineWhere(baseWhere, additionalWhere)
  const params = qb.getParams()

  const query = `
    SELECT *
    FROM ${this.table} FINAL
    WHERE ${whereClause}
    ORDER BY createdAt DESC
    LIMIT 1
  `

  const result = await this.client.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = (await result.json())

  if (rows.length === 0) {
    return null
  }

  const parsedRow = parseDataRow(rows[0]!)
  return rowToDocument<T>(parsedRow)
}
