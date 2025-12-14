import type { CountArgs, CountVersions } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'

const VERSIONS_TYPE_PREFIX = '_versions_'

export const countVersions: CountVersions = async function countVersions(
  this: ClickHouseAdapter,
  args: CountArgs,
): Promise<{ totalDocs: number }> {
  const { collection: collectionSlug, where } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const versionType = `${VERSIONS_TYPE_PREFIX}${collectionSlug}`

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
