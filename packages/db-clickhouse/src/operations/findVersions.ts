import type {
  FindVersions,
  FindVersionsArgs,
  JsonObject,
  PaginatedDocs,
  TypeWithVersion,
} from 'payload'

import type { ClickHouseAdapter, DataRow } from '../types.js'

import { buildLimitOffset, buildOrderBy } from '../queries/buildSort.js'
import { QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { parseDataRow } from '../utilities/transform.js'

const VERSIONS_TYPE_PREFIX = '_versions_'

export const findVersions: FindVersions = async function findVersions<T = JsonObject>(
  this: ClickHouseAdapter,
  args: FindVersionsArgs,
): Promise<PaginatedDocs<TypeWithVersion<T>>> {
  const { collection: collectionSlug, limit = 10, page = 1, pagination = true, sort, where } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.client) {
    throw new Error('ClickHouse client not connected')
  }

  const versionType = `${VERSIONS_TYPE_PREFIX}${collectionSlug}`

  // Build parameterized base where
  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)
  const typeParam = qb.addNamedParam('versionType', versionType)
  const baseWhere = `ns = ${nsParam} AND type = ${typeParam} AND deletedAt IS NULL`

  // Check if querying for latest - we compute this dynamically via max(v)
  const hasLatestQuery = where && 'latest' in where
  const latestValue = hasLatestQuery ? (where as any).latest?.equals : undefined

  // Remove 'latest' from where clause since we handle it specially
  const filteredWhere = where ? { ...where } : {}
  if ('latest' in filteredWhere) {
    delete (filteredWhere as any).latest
  }

  const additionalWhere = qb.buildWhereClause(filteredWhere as any)
  const whereClause = additionalWhere ? `${baseWhere} AND (${additionalWhere})` : baseWhere
  const params = qb.getParams()

  let sortString: string | undefined = '-createdAt'
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

  let query: string
  let countQuery: string

  if (latestValue === true) {
    // Use window function with FINAL to find only the latest version per parent
    query = `
      SELECT *
      FROM (
        SELECT *,
          row_number() OVER (PARTITION BY data._parentId ORDER BY v DESC) as _rn
        FROM ${this.table} FINAL
        WHERE ${whereClause}
      )
      WHERE _rn = 1
      ${orderBy}
      ${limitOffset}
    `
    countQuery = `
      SELECT count() as total
      FROM (
        SELECT data._parentId,
          row_number() OVER (PARTITION BY data._parentId ORDER BY v DESC) as _rn
        FROM ${this.table} FINAL
        WHERE ${whereClause}
      )
      WHERE _rn = 1
    `
  } else if (latestValue === false) {
    // Find all non-latest versions (exclude the max v per parent)
    query = `
      SELECT *
      FROM (
        SELECT *,
          row_number() OVER (PARTITION BY data._parentId ORDER BY v DESC) as _rn
        FROM ${this.table} FINAL
        WHERE ${whereClause}
      )
      WHERE _rn > 1
      ${orderBy}
      ${limitOffset}
    `
    countQuery = `
      SELECT count() as total
      FROM (
        SELECT data._parentId,
          row_number() OVER (PARTITION BY data._parentId ORDER BY v DESC) as _rn
        FROM ${this.table} FINAL
        WHERE ${whereClause}
      )
      WHERE _rn > 1
    `
  } else {
    // No latest filter - return all versions
    query = `
      SELECT *
      FROM ${this.table} FINAL
      WHERE ${whereClause}
      ${orderBy}
      ${limitOffset}
    `
    countQuery = `
      SELECT count() as total
      FROM ${this.table} FINAL
      WHERE ${whereClause}
    `
  }

  const result = await this.client.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = (await result.json())

  const docs = rows.map((row: DataRow): TypeWithVersion<T> => {
    const parsed = parseDataRow(row)
    const data = parsed.data
    const { _autosave, _parentId, ...versionData } = data

    return {
      id: row.id,
      createdAt: row.createdAt,
      parent: _parentId as number | string,
      updatedAt: row.updatedAt,
      version: versionData as T,
    }
  })

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
