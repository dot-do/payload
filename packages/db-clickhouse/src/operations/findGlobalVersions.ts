import type {
  FindGlobalVersions,
  FindGlobalVersionsArgs,
  JsonObject,
  PaginatedDocs,
  TypeWithVersion,
} from 'payload'

import type { ClickHouseAdapter, DataRow } from '../types.js'

import { buildLimitOffset, buildOrderBy } from '../queries/buildSort.js'
import { QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { parseDataRow } from '../utilities/transform.js'

const GLOBAL_VERSIONS_TYPE_PREFIX = '_global_versions_'

export const findGlobalVersions: FindGlobalVersions = async function findGlobalVersions<
  T = JsonObject,
>(
  this: ClickHouseAdapter,
  args: FindGlobalVersionsArgs,
): Promise<PaginatedDocs<TypeWithVersion<T>>> {
  const { global: globalSlug, limit = 10, page = 1, pagination = true, sort, where } = args

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

  const docs = rows.map((row: DataRow): TypeWithVersion<T> => {
    const parsed = parseDataRow(row)
    const data = parsed.data
    const { _autosave, _globalSlug, ...versionData } = data

    return {
      id: row.id,
      createdAt: row.createdAt,
      parent: globalSlug,
      updatedAt: row.updatedAt,
      version: versionData as T,
    }
  })

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
