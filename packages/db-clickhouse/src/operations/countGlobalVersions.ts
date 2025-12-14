import type { CountGlobalVersionArgs, CountGlobalVersions } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'

const GLOBAL_VERSIONS_TYPE_PREFIX = '_global_versions_'

export const countGlobalVersions: CountGlobalVersions = async function countGlobalVersions(
  this: ClickHouseAdapter,
  args: CountGlobalVersionArgs,
): Promise<{ totalDocs: number }> {
  const { global: globalSlug, where } = args

  assertValidSlug(globalSlug, 'global')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const versionType = `${GLOBAL_VERSIONS_TYPE_PREFIX}${globalSlug}`

  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)
  const typeParam = qb.addNamedParam('versionType', versionType)
  const baseWhere = `ns = ${nsParam} AND type = ${typeParam} AND deletedAt IS NULL`

  const additionalWhere = qb.buildWhereClause(where as any)
  const whereClause = additionalWhere ? `${baseWhere} AND (${additionalWhere})` : baseWhere
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
