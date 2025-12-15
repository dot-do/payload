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

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const qb = new QueryBuilder()
  const baseWhereInner = qb.buildBaseWhereNoDeleted(this.namespace, collectionSlug)
  const additionalWhere = qb.buildWhereClause(where as any)
  const innerWhereClause = combineWhere(baseWhereInner, additionalWhere)
  const params = qb.getParams()

  // Count only the latest versions, excluding soft-deleted
  const query = `
    SELECT count() as total
    FROM (
      SELECT id, deletedAt, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ${innerWhereClause}
    )
    WHERE _rn = 1 AND deletedAt IS NULL
  `

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = await result.json<{ total: string }>()
  const totalDocs = parseInt(rows[0]?.total || '0', 10)

  return { totalDocs }
}
