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
import {
  parseDataRow,
  parseDateTime64ToMs,
  stripSensitiveFields,
  toISOString,
} from '../utilities/transform.js'

/**
 * Get the versions collection type name.
 * Payload stores versions with the naming convention: _${collection}_versions
 */
function getVersionsType(collectionSlug: string): string {
  return `_${collectionSlug}_versions`
}

/**
 * Find versions of documents.
 *
 * Versions are stored with type = `_${collection}_versions`.
 * Each version has a unique `v` timestamp that serves as its identifier.
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

  // Build parameterized base where using versions type: _${collection}_versions
  const versionsType = getVersionsType(collectionSlug)
  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)
  const typeParam = qb.addNamedParam('type', versionsType)
  const baseWhere = `ns = ${nsParam} AND type = ${typeParam} AND deletedAt IS NULL`

  // Check if querying for latest - we compute this dynamically via max(v)
  const hasLatestQuery = where && 'latest' in where
  const latestValue = hasLatestQuery ? (where as any).latest?.equals : undefined

  // Remove 'latest' from where clause since we handle it specially
  // Keep 'parent' queries as-is since parent is stored in the data JSON
  const filteredWhere: Record<string, unknown> = where ? { ...where } : {}
  if ('latest' in filteredWhere) {
    delete filteredWhere.latest
  }

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

  // FINAL ensures rows with same (ns, type, id, v) are deduplicated
  // This is necessary because updateVersion inserts a row with the same v
  // and without FINAL, both the old and new row would be visible until background merge
  // Note: Different versions have different v values, so they are NOT deduplicated against each other

  if (latestValue === true) {
    // Use window function to find only the latest version per document id
    query = `
      SELECT * EXCEPT(_rn)
      FROM (
        SELECT *,
          row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
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
        SELECT id,
          row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
        FROM ${this.table} FINAL
        WHERE ${whereClause}
      )
      WHERE _rn = 1
    `
  } else if (latestValue === false) {
    // Find all non-latest versions (exclude the max v per document)
    query = `
      SELECT * EXCEPT(_rn)
      FROM (
        SELECT *,
          row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
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
        SELECT id,
          row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
        FROM ${this.table} FINAL
        WHERE ${whereClause}
      )
      WHERE _rn > 1
    `
  } else {
    // No latest filter - return ALL version rows (each row is a version)
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

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = await result.json<DataRow>()

  const docs = rows.map((row): TypeWithVersion<T> => {
    const parsed = parseDataRow(row)
    const data = parsed.data as { _autosave?: boolean; parent: string; version: T }

    // Strip sensitive fields from version data
    const sanitizedVersion = stripSensitiveFields(data.version as Record<string, unknown>) as T

    return {
      id: row.id, // Version ID is the row ID (v timestamp as string)
      createdAt: row.createdAt
        ? toISOString(row.createdAt) || new Date().toISOString()
        : new Date().toISOString(),
      parent: data.parent,
      updatedAt: row.updatedAt
        ? toISOString(row.updatedAt) || new Date().toISOString()
        : new Date().toISOString(),
      version: sanitizedVersion,
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
