import type { ClickHouseAdapter, UpdateSearchStatusArgs } from '../types.js'

import { generateVersion } from '../utilities/generateId.js'

export async function updateSearchStatus(
  this: ClickHouseAdapter,
  args: UpdateSearchStatusArgs,
): Promise<void> {
  const { id, embedding, error, status } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const now = generateVersion()

  // For ReplacingMergeTree, we need to insert a new row with updated values
  // First, get the existing row
  const selectQuery = `
    SELECT id, ns, collection, docId, chunkIndex, text, createdAt
    FROM search
    WHERE id = {id:String} AND ns = {ns:String}
    ORDER BY updatedAt DESC
    LIMIT 1
  `

  const selectResult = await this.clickhouse.query({
    format: 'JSONEachRow',
    query: selectQuery,
    query_params: { id, ns: this.namespace },
  })

  const rows = await selectResult.json<{
    chunkIndex: number
    collection: string
    createdAt: string
    docId: string
    id: string
    ns: string
    text: string
  }>()

  if (rows.length === 0) {
    throw new Error(`Search document with id '${id}' not found`)
  }

  const row = rows[0]!
  const embeddingValue = embedding || new Array(this.embeddingDimensions).fill(0)

  const insertQuery = `
    INSERT INTO search (id, ns, collection, docId, chunkIndex, text, embedding, status, errorMessage, createdAt, updatedAt)
    VALUES (
      {id:String},
      {ns:String},
      {collection:String},
      {docId:String},
      {chunkIndex:UInt16},
      {text:String},
      {embedding:Array(Float32)},
      {status:String},
      ${error ? '{errorMessage:String}' : 'NULL'},
      parseDateTimeBestEffort({createdAt:String}),
      fromUnixTimestamp64Milli({updatedAt:Int64})
    )
  `

  const params: Record<string, unknown> = {
    id: row.id,
    chunkIndex: row.chunkIndex,
    collection: row.collection,
    createdAt: row.createdAt,
    docId: row.docId,
    embedding: embeddingValue,
    ns: row.ns,
    status,
    text: row.text,
    updatedAt: now,
  }

  if (error) {
    params.errorMessage = error
  }

  await this.clickhouse.command({
    query: insertQuery,
    query_params: params,
  })
}
