import type { Count, CountArgs } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

import { combineWhere, QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'

export const count: Count = async function count(
  this: ClickHouseAdapter,
  args: CountArgs,
): Promise<{ totalDocs: number }> {
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
    SELECT count() as total
    FROM ${this.table} FINAL
    WHERE ${whereClause}
  `

  const result = await this.client.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = (await result.json())
  const totalDocs = parseInt(rows[0]?.total || '0', 10)

  return { totalDocs }
}
