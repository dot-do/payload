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
import { parseDataRow, parseDateTime64ToMs, toISOString } from '../utilities/transform.js'

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
      // Map 'parent' to 'id' - in ClickHouse versioning, parent IS the document id
      result.id = value
    } else {
      result[key] = value
    }
  }

  return result
}

/**
 * Find versions of documents.
 *
 * ClickHouse-native versioning: versions are rows with the same (ns, type, id)
 * but different `v` timestamps. The `v` timestamp serves as the version identifier.
 */
export const findVersions: FindVersions = async function findVersions<T = JsonObject>(
  this: ClickHouseAdapter,
  args: FindVersionsArgs,
): Promise<PaginatedDocs<TypeWithVersion<T>>> {
  const { collection: collectionSlug, limit = 10, page = 1, pagination = true, sort, where } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  // Build parameterized base where - use collection type directly (no _versions_ prefix)
  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)
  const typeParam = qb.addNamedParam('type', collectionSlug)
  const baseWhere = `ns = ${nsParam} AND type = ${typeParam} AND deletedAt IS NULL`

  // Check if querying for latest - we compute this dynamically via max(v)
  const hasLatestQuery = where && 'latest' in where
  const latestValue = hasLatestQuery ? (where as any).latest?.equals : undefined

  // Remove 'latest' from where clause since we handle it specially
  // Map 'parent' to 'id' since in ClickHouse versioning, parent IS the document id
  let filteredWhere: Record<string, unknown> = where ? { ...where } : {}
  if ('latest' in filteredWhere) {
    delete filteredWhere.latest
  }
  // Recursively map 'parent' to 'id' throughout the where clause
  filteredWhere = mapParentToId(filteredWhere)

  const additionalWhere = qb.buildWhereClause(filteredWhere as any)
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

  let query: string
  let countQuery: string

  if (latestValue === true) {
    // Use window function to find only the latest version per document id
    query = `
      SELECT *
      FROM (
        SELECT *,
          row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
        FROM ${this.table}
        WHERE ${whereClause}
      )
      WHERE _rn = 1
      ${orderBy}
      ${limitOffset}
    `
    countQuery = `
      SELECT count() as total
      FROM (
        SELECT id,
          row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
        FROM ${this.table}
        WHERE ${whereClause}
      )
      WHERE _rn = 1
    `
  } else if (latestValue === false) {
    // Find all non-latest versions (exclude the max v per document)
    query = `
      SELECT *
      FROM (
        SELECT *,
          row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
        FROM ${this.table}
        WHERE ${whereClause}
      )
      WHERE _rn > 1
      ${orderBy}
      ${limitOffset}
    `
    countQuery = `
      SELECT count() as total
      FROM (
        SELECT id,
          row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
        FROM ${this.table}
        WHERE ${whereClause}
      )
      WHERE _rn > 1
    `
  } else {
    // No latest filter - return all versions
    query = `
      SELECT *
      FROM ${this.table}
      WHERE ${whereClause}
      ${orderBy}
      ${limitOffset}
    `
    countQuery = `
      SELECT count() as total
      FROM ${this.table}
      WHERE ${whereClause}
    `
  }

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
      parent: row.id,
      updatedAt: row.updatedAt
        ? toISOString(row.updatedAt) || new Date().toISOString()
        : new Date().toISOString(),
      version: versionData as T,
    }
  })

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
