import type { FindDistinct, PaginatedDistinctDocs } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

import { buildOrderBy } from '../queries/buildSort.js'
import { QueryBuilder } from '../queries/QueryBuilder.js'
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
  const { collection: collectionSlug, field, limit = 10, page = 1, sort, where } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const qb = new QueryBuilder()
  // Build base WHERE for inner query (only ns, type)
  // Data field filters must be applied AFTER window function
  const baseWhereInner = qb.buildBaseWhereNoDeleted(this.namespace, collectionSlug)
  const dataWhere = qb.buildWhereClause(where as any)

  const fieldPath = getFieldPath(field)
  // Cast JSON fields to String to avoid "Dynamic type not allowed in ORDER BY" error
  const fieldExpr = fieldPath.startsWith('data.') ? `toString(${fieldPath})` : fieldPath

  const effectiveLimit = limit
  const effectivePage = page
  const offset = (effectivePage - 1) * effectiveLimit

  // Add LIMIT and OFFSET as parameterized values for consistency
  const limitParam = effectiveLimit > 0 ? qb.addParam(effectiveLimit, 'Int64') : null
  const offsetParam = effectiveLimit > 0 ? qb.addParam(offset, 'Int64') : null

  // Get params AFTER all parameters have been added
  const params = qb.getParams()

  // Build ORDER BY clause based on sort parameter
  // The sort can be the field name or '-field' for descending
  let orderByClause = 'ORDER BY value ASC'
  if (sort) {
    const sortStr = typeof sort === 'string' ? sort : String(sort)
    // Check if descending (starts with -)
    if (sortStr.startsWith('-')) {
      orderByClause = 'ORDER BY value DESC'
    } else {
      orderByClause = 'ORDER BY value ASC'
    }
  }

  // Use window function, filter deletedAt after
  // Apply data filters AFTER window function to ensure we filter on latest version
  const outerWhere = dataWhere
    ? `_rn = 1 AND deletedAt IS NULL AND (${dataWhere})`
    : '_rn = 1 AND deletedAt IS NULL'
  const query = `
    SELECT DISTINCT ${fieldExpr} as value
    FROM (
      SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ${baseWhereInner}
    )
    WHERE ${outerWhere}
    ${orderByClause}
    ${limitParam && offsetParam ? `LIMIT ${limitParam} OFFSET ${offsetParam}` : ''}
  `

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = await result.json<{ value: unknown }>()
  const values = rows.map((row) => ({ [field]: row.value }))

  // Get total count of distinct values (only from latest versions, excluding deleted)
  const countQuery = `
    SELECT count(DISTINCT ${fieldExpr}) as total
    FROM (
      SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ${baseWhereInner}
    )
    WHERE ${outerWhere}
  `

  const countResult = await this.clickhouse.query({
    format: 'JSONEachRow',
    query: countQuery,
    query_params: params,
  })

  const countRows = await countResult.json<{ total: string }>()
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
