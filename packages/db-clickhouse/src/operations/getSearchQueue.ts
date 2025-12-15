import type { ClickHouseAdapter, GetSearchQueueArgs, SearchQueueItem } from '../types.js'

export async function getSearchQueue(
  this: ClickHouseAdapter,
  args: GetSearchQueueArgs = {},
): Promise<SearchQueueItem[]> {
  const { limit = 100 } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const query = `
    SELECT id, collection, docId, text
    FROM search
    WHERE ns = {ns:String} AND status = 'pending'
    ORDER BY createdAt ASC
    LIMIT ${limit}
  `

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params: { ns: this.namespace },
  })

  return result.json<SearchQueueItem>()
}
