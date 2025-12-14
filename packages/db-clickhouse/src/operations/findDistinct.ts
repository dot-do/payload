import type { FindDistinct, PaginatedDistinctDocs } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

import { combineWhere, QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'

/**
 * Get the ClickHouse field path for a Payload field
 */
function getFieldPath(field: string): string {
  const sanitized = field.replace(/[^\w.[\]]/g, '')

  const topLevelFields = [
    'id',
    'ns',
    'type',
    'v',
    'title',
    'createdAt',
    'createdBy',
    'updatedAt',
    'updatedBy',
    'deletedAt',
    'deletedBy',
  ]

  if (topLevelFields.includes(sanitized)) {
    return sanitized
  }

  if (sanitized.startsWith('version.')) {
    const versionField = sanitized.slice('version.'.length)
    return `data.${versionField}`
  }

  return `data.${sanitized}`
}

export const findDistinct: FindDistinct = async function findDistinct(
  this: ClickHouseAdapter,
  args: Parameters<FindDistinct>[0],
): Promise<PaginatedDistinctDocs<Record<string, any>>> {
  const { collection: collectionSlug, field, limit = 10, page = 1, where } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const qb = new QueryBuilder()
  const baseWhere = qb.buildBaseWhere(this.namespace, collectionSlug)
  const additionalWhere = qb.buildWhereClause(where as any)
  const whereClause = combineWhere(baseWhere, additionalWhere)
  const params = qb.getParams()

  const fieldPath = getFieldPath(field)

  const effectiveLimit = limit
  const effectivePage = page
  const offset = (effectivePage - 1) * effectiveLimit

  const query = `
    SELECT DISTINCT ${fieldPath} as value
    FROM ${this.table} FINAL
    WHERE ${whereClause}
    ORDER BY value
    ${effectiveLimit > 0 ? `LIMIT ${effectiveLimit} OFFSET ${offset}` : ''}
  `

  const result = await this.client.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = (await result.json())
  const values = rows.map((row: { value: unknown }) => ({ [field]: row.value }))

  // Get total count
  const countQuery = `
    SELECT count(DISTINCT ${fieldPath}) as total
    FROM ${this.table} FINAL
    WHERE ${whereClause}
  `

  const countResult = await this.client.query({
    format: 'JSONEachRow',
    query: countQuery,
    query_params: params,
  })

  const countRows = (await countResult.json())
  const totalDocs = parseInt(countRows[0]?.total || '0', 10)
  const totalPages = effectiveLimit > 0 ? Math.ceil(totalDocs / effectiveLimit) : 1
  const hasNextPage = effectivePage < totalPages
  const hasPrevPage = effectivePage > 1

  return {
    hasNextPage,
    hasPrevPage,
    limit: effectiveLimit,
    nextPage: hasNextPage ? effectivePage + 1 : null,
    page: effectivePage,
    pagingCounter: (effectivePage - 1) * effectiveLimit + 1,
    prevPage: hasPrevPage ? effectivePage - 1 : null,
    totalDocs,
    totalPages,
    values,
  }
}
