import type { Find, FindArgs, PaginatedDocs, TypeWithID } from 'payload'

import type { ClickHouseAdapter, DataRow } from '../types.js'

import { buildLimitOffset, buildOrderBy } from '../queries/buildSort.js'
import { QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { resolveJoins } from '../utilities/resolveJoins.js'
import { hasCustomNumericID, parseDataRow, rowsToDocuments } from '../utilities/transform.js'

export const find: Find = async function find<T = TypeWithID>(
  this: ClickHouseAdapter,
  args: FindArgs,
): Promise<PaginatedDocs<T>> {
  const {
    collection: collectionSlug,
    joins,
    limit = 10,
    locale,
    page = 1,
    pagination = true,
    sort,
    where,
  } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const collection = this.payload.collections[collectionSlug]
  const numericID = collection ? hasCustomNumericID(collection.config.fields) : false

  const qb = new QueryBuilder()
  // Build base WHERE for inner query (only ns, type - no data field filters)
  // Data field filters must be applied AFTER window function to ensure we filter
  // on the LATEST version of each document, not old versions
  const baseWhereInner = qb.buildBaseWhereNoDeleted(this.namespace, collectionSlug)
  const dataWhere = qb.buildWhereClause(where as any)
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
  // IMPORTANT: Data field filters (dataWhere) are applied AFTER window function
  // This ensures we filter on the LATEST version of each document, not old versions
  // Without this, queries could return stale data if an older version matches but the latest doesn't
  const outerWhere = dataWhere
    ? `_rn = 1 AND deletedAt IS NULL AND (${dataWhere})`
    : '_rn = 1 AND deletedAt IS NULL'
  const query = `
    SELECT * EXCEPT(_rn)
    FROM (
      SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ${baseWhereInner}
    )
    WHERE ${outerWhere}
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
  const docs = rowsToDocuments<T & TypeWithID>(parsedRows, numericID) as T[]

  // Resolve join fields
  await resolveJoins({
    adapter: this,
    collectionSlug,
    docs: docs as Record<string, unknown>[],
    joins,
    locale,
  })

  // Count only the latest versions (not all versions), excluding soft-deleted
  // Apply data filters after window function to count latest versions only
  const countOuterWhere = dataWhere
    ? `_rn = 1 AND deletedAt IS NULL AND (${dataWhere})`
    : '_rn = 1 AND deletedAt IS NULL'
  const countQuery = `
    SELECT count() as total
    FROM (
      SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ${baseWhereInner}
    )
    WHERE ${countOuterWhere}
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
