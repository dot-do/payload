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

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const qb = new QueryBuilder()
  const baseWhere = qb.buildBaseWhere(this.namespace, collectionSlug)
  const additionalWhere = qb.buildWhereClause(where as any)
  const whereClause = combineWhere(baseWhere, additionalWhere)
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
  const effectiveLimit = pagination ? limit : 0
  const effectivePage = pagination ? page : 1
  const limitOffset = buildLimitOffset(effectiveLimit, effectivePage)

  const query = `
    SELECT *
    FROM ${this.table} FINAL
    WHERE ${whereClause}
    ${orderBy}
    ${limitOffset}
  `

  const result = await this.client.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = (await result.json())
  const parsedRows = rows.map(parseDataRow)
  const docs = rowsToDocuments<T & TypeWithID>(parsedRows) as T[]

  const countQuery = `
    SELECT count() as total
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
