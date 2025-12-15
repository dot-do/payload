import type { CountArgs, CountVersions } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'

/**
 * Recursively map 'parent' field to 'id' in where clause.
 * In ClickHouse-native versioning, 'parent' is the document id.
 */
function mapParentToId(where: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {}

  for (const [key, value] of Object.entries(where)) {
    if (key === 'and' && Array.isArray(value)) {
      result.and = value.map((item) =>
        typeof item === 'object' && item !== null
          ? mapParentToId(item as Record<string, unknown>)
          : item,
      )
    } else if (key === 'or' && Array.isArray(value)) {
      result.or = value.map((item) =>
        typeof item === 'object' && item !== null
          ? mapParentToId(item as Record<string, unknown>)
          : item,
      )
    } else if (key === 'parent') {
      result.id = value
    } else {
      result[key] = value
    }
  }

  return result
}

/**
 * Count versions of documents.
 *
 * ClickHouse-native versioning: versions are rows with the same (ns, type, id)
 * but different `v` timestamps.
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

  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)
  const typeParam = qb.addNamedParam('type', collectionSlug)
  const baseWhere = `ns = ${nsParam} AND type = ${typeParam} AND deletedAt IS NULL`

  // Recursively map 'parent' to 'id' since that's how versions are stored
  const filteredWhere = where ? mapParentToId(where as Record<string, unknown>) : {}

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
