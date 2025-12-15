import type { ClickHouseAdapter, SearchArgs, SearchResult, SearchResultDoc } from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'

export async function search(
  this: ClickHouseAdapter,
  args: SearchArgs = {},
): Promise<SearchResult> {
  const { hybrid, limit = 10, text, vector, where } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  if (!text && !vector) {
    return { docs: [] }
  }

  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)

  let whereClause = `ns = ${nsParam} AND status = 'ready'`
  const accessWhere = qb.buildWhereClause(where)
  if (accessWhere) {
    whereClause = `${whereClause} AND (${accessWhere})`
  }

  // Get params from QueryBuilder (only primitives)
  const baseParams = qb.getParams()

  let query: string
  let scoreExpr: string
  // ClickHouse params that include arrays
  const queryParams: Record<string, unknown> = { ...baseParams }

  if (text && vector && hybrid) {
    // Hybrid search: combine text and vector scores
    const textWeight = hybrid.textWeight
    const vectorWeight = hybrid.vectorWeight
    queryParams.searchText = text.toLowerCase()
    queryParams.searchVector = vector

    scoreExpr = `(
      ${textWeight} * (CASE WHEN position(lower(text), {searchText:String}) > 0 THEN 1 ELSE 0 END) +
      ${vectorWeight} * (1 - cosineDistance(embedding, {searchVector:Array(Float32)}))
    ) as score`

    query = `
      SELECT id, collection, docId, chunkIndex, text, ${scoreExpr}
      FROM search
      WHERE ${whereClause}
      ORDER BY score DESC
      LIMIT ${limit}
    `
  } else if (vector) {
    // Vector-only search
    queryParams.searchVector = vector

    query = `
      SELECT id, collection, docId, chunkIndex, text,
        (1 - cosineDistance(embedding, {searchVector:Array(Float32)})) as score
      FROM search
      WHERE ${whereClause}
      ORDER BY cosineDistance(embedding, {searchVector:Array(Float32)})
      LIMIT ${limit}
    `
  } else {
    // Text-only search
    queryParams.searchText = text!.toLowerCase()

    query = `
      SELECT id, collection, docId, chunkIndex, text,
        (CASE WHEN position(lower(text), {searchText:String}) > 0 THEN 1 ELSE 0 END) as score
      FROM search
      WHERE ${whereClause} AND position(lower(text), {searchText:String}) > 0
      ORDER BY position(lower(text), {searchText:String})
      LIMIT ${limit}
    `
  }

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params: queryParams,
  })

  const rows = await result.json<SearchResultDoc>()

  return { docs: rows }
}
