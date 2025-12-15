import type { ClickHouseAdapter, ExecuteArgs } from '../types.js'

/**
 * Execute a raw SQL query against ClickHouse
 * Returns results as an array of typed objects
 */
export async function execute<T = unknown>(
  this: ClickHouseAdapter,
  args: ExecuteArgs<T>,
): Promise<T[]> {
  const { query, query_params } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params,
  })

  return result.json<T>()
}
