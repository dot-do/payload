import type { CountArgs, CountVersions } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'

/**
 * Get the versions collection type name.
 * Payload stores versions with the naming convention: _${collection}_versions
 */
function getVersionsType(collectionSlug: string): string {
  return `_${collectionSlug}_versions`
}

/**
 * Count versions of documents.
 *
 * Versions are stored with type = `_${collection}_versions`.
 */
export const countVersions: CountVersions = async function countVersions(
  this: ClickHouseAdapter,
  args: CountArgs,
): Promise<{ totalDocs: number }> {
  const { collection: collectionSlug, where } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  // Use versions type: _${collection}_versions
  const versionsType = getVersionsType(collectionSlug)
  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)
  const typeParam = qb.addNamedParam('type', versionsType)
  const baseWhere = `ns = ${nsParam} AND type = ${typeParam} AND deletedAt IS NULL`

  // Keep where as-is since parent is stored in the data JSON
  const filteredWhere = where || {}

  const additionalWhere = qb.buildWhereClause(filteredWhere as any)
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
