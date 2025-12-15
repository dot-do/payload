import type { ClickHouseAdapter, EventRow, QueryEventsArgs, QueryEventsResult } from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'

export async function queryEvents(
  this: ClickHouseAdapter,
  args: QueryEventsArgs = {},
): Promise<QueryEventsResult> {
  const { limit = 10, page = 1, sort = '-timestamp', where } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)

  let whereClause = `ns = ${nsParam}`
  const accessWhere = qb.buildWhereClause(where)
  if (accessWhere) {
    whereClause = `${whereClause} AND (${accessWhere})`
  }

  // Parse sort field
  const sortDesc = sort.startsWith('-')
  const sortField = sortDesc ? sort.slice(1) : sort
  const orderBy = `ORDER BY ${sortField} ${sortDesc ? 'DESC' : 'ASC'}`

  const offset = (page - 1) * limit
  const params = qb.getParams()

  // Count query
  const countQuery = `SELECT count() as total FROM events WHERE ${whereClause}`
  const countResult = await this.clickhouse.query({
    format: 'JSONEachRow',
    query: countQuery,
    query_params: params,
  })
  const countRows = await countResult.json<{ total: string }>()
  const totalDocs = parseInt(countRows[0]?.total || '0', 10)

  // Data query
  const dataQuery = `
    SELECT *
    FROM events
    WHERE ${whereClause}
    ${orderBy}
    LIMIT ${limit}
    OFFSET ${offset}
  `

  const dataResult = await this.clickhouse.query({
    format: 'JSONEachRow',
    query: dataQuery,
    query_params: params,
  })

  const rows = await dataResult.json<EventRow>()

  // Parse JSON fields
  const docs = rows.map((row) => ({
    ...row,
    input: typeof row.input === 'string' ? JSON.parse(row.input) : row.input,
    result: typeof row.result === 'string' ? JSON.parse(row.result) : row.result,
  }))

  const totalPages = Math.ceil(totalDocs / limit)

  return {
    docs,
    hasNextPage: page < totalPages,
    hasPrevPage: page > 1,
    limit,
    nextPage: page < totalPages ? page + 1 : null,
    page,
    prevPage: page > 1 ? page - 1 : null,
    totalDocs,
    totalPages,
  }
}
