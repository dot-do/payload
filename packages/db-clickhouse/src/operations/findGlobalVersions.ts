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
import { parseDataRow, parseDateTime64ToMs, toISOString } from '../utilities/transform.js'

const GLOBAL_TYPE_PREFIX = '_global_'

/**
 * Find versions of a global.
 *
 * ClickHouse-native versioning: versions are rows with the same (ns, type, id)
 * but different `v` timestamps. For globals, id is the global slug.
 */
export const findGlobalVersions: FindGlobalVersions = async function findGlobalVersions<
  T = JsonObject,
>(
  this: ClickHouseAdapter,
  args: FindGlobalVersionsArgs,
): Promise<PaginatedDocs<TypeWithVersion<T>>> {
  const { global: globalSlug, limit = 10, page = 1, pagination = true, sort, where } = args

  assertValidSlug(globalSlug, 'global')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const globalType = `${GLOBAL_TYPE_PREFIX}${globalSlug}`

  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)
  const typeParam = qb.addNamedParam('type', globalType)
  const baseWhere = `ns = ${nsParam} AND type = ${typeParam} AND deletedAt IS NULL`

  const additionalWhere = qb.buildWhereClause(where as any)
  const whereClause = additionalWhere ? `${baseWhere} AND (${additionalWhere})` : baseWhere
  const params = qb.getParams()

  // Default sort by v descending (newest versions first)
  let sortString: string | undefined = '-v'
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
    FROM ${this.table}
    WHERE ${whereClause}
    ${orderBy}
    ${limitOffset}
  `

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = await result.json<DataRow>()

  const docs = rows.map((row): TypeWithVersion<T> => {
    const parsed = parseDataRow(row)
    const data = parsed.data
    // Remove internal fields from version data
    const { _autosave, ...versionData } = data

    // Convert v timestamp to milliseconds for version id
    const vTimestamp = row.v ? parseDateTime64ToMs(row.v) : Date.now()

    return {
      id: String(vTimestamp),
      createdAt: row.createdAt
        ? toISOString(row.createdAt) || new Date().toISOString()
        : new Date().toISOString(),
      parent: globalSlug,
      updatedAt: row.updatedAt
        ? toISOString(row.updatedAt) || new Date().toISOString()
        : new Date().toISOString(),
      version: versionData as T,
    }
  })

  const countQuery = `
    SELECT count() as total
    FROM ${this.table}
    WHERE ${whereClause}
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
