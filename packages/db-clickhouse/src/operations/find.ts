import type { Find, FindArgs, PaginatedDocs, TypeWithID } from 'payload'

import type { ClickHouseAdapter, DataRow } from '../types.js'

import { buildLimitOffset, buildOrderBy } from '../queries/buildSort.js'
import { combineWhere, QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { parseDataRow, rowsToDocuments } from '../utilities/transform.js'

export const find: Find = async function find<T = TypeWithID>(
  this: ClickHouseAdapter,
  args: FindArgs,
): Promise<PaginatedDocs<T>> {
  const { collection: collectionSlug, limit = 10, page = 1, pagination = true, sort, where } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const qb = new QueryBuilder()
  // Build base WHERE without deletedAt (for use inside subquery)
  const baseWhereInner = qb.buildBaseWhereNoDeleted(this.namespace, collectionSlug)
  const additionalWhere = qb.buildWhereClause(where as any)
  const innerWhereClause = combineWhere(baseWhereInner, additionalWhere)
  const params = qb.getParams()

  let sortString: string | undefined
  if (typeof sort === 'string') {
    sortString = sort
  } else if (Array.isArray(sort)) {
    sortString = sort.join(',')
  } else if (sort && typeof sort === 'object') {
    sortString = Object.entries(sort)
      .map(([key, dir]) => (dir === 'desc' || dir === -1 ? `-${key}` : key))
      .join(',')
  }

  const orderBy = buildOrderBy(sortString)
  // When pagination is disabled, still respect limit if explicitly set (for bulk operations)
  // A limit of 0 means no limit (return all)
  const effectiveLimit = pagination ? limit : limit > 0 ? limit : 0
  const effectivePage = pagination ? page : 1
  const limitOffset = buildLimitOffset(effectiveLimit, effectivePage)

  // Use window function to get latest version per document
  // Apply deletedAt filter AFTER window function so soft-deleted docs are properly excluded
  // Note: We don't use FINAL because ORDER BY includes v, so each version is unique
  const query = `
    SELECT * EXCEPT(_rn)
    FROM (
      SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ${innerWhereClause}
    )
    WHERE _rn = 1 AND deletedAt IS NULL
    ${orderBy}
    ${limitOffset}
  `

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = await result.json<DataRow>()
  const parsedRows = rows.map(parseDataRow)
  const docs = rowsToDocuments<T & TypeWithID>(parsedRows) as T[]

  // Count only the latest versions (not all versions), excluding soft-deleted
  const countQuery = `
    SELECT count() as total
    FROM (
      SELECT id, deletedAt, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ${innerWhereClause}
    )
    WHERE _rn = 1 AND deletedAt IS NULL
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
    docs,
    hasNextPage,
    hasPrevPage,
    limit: effectiveLimit,
    nextPage: hasNextPage ? effectivePage + 1 : null,
    page: effectivePage,
    pagingCounter: (effectivePage - 1) * effectiveLimit + 1,
    prevPage: hasPrevPage ? effectivePage - 1 : null,
    totalDocs,
    totalPages,
  }
}
