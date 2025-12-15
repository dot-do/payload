import type { CountGlobalVersionArgs, CountGlobalVersions } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'

const GLOBAL_TYPE_PREFIX = '_global_'

/**
 * Count versions of a global.
 *
 * ClickHouse-native versioning: versions are rows with the same (ns, type, id)
 * but different `v` timestamps.
 */
export const countGlobalVersions: CountGlobalVersions = async function countGlobalVersions(
  this: ClickHouseAdapter,
  args: CountGlobalVersionArgs,
): Promise<{ totalDocs: number }> {
  const { global: globalSlug, where } = args

  assertValidSlug(globalSlug, 'global')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const globalType = `${GLOBAL_TYPE_PREFIX}${globalSlug}`

  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)
  const typeParam = qb.addNamedParam('type', globalType)
  const baseWhere = `ns = ${nsParam} AND type = ${typeParam} AND deletedAt IS NULL`

  const additionalWhere = qb.buildWhereClause(where as any)
  const whereClause = additionalWhere ? `${baseWhere} AND (${additionalWhere})` : baseWhere
  const params = qb.getParams()

  const query = `
    SELECT count() as total
    FROM ${this.table}
    WHERE ${whereClause}
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
