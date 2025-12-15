import { createClient } from '@clickhouse/client-web'

import type { ClickHouseAdapter } from '../../types.js'

/**
 * Create a minimal test adapter for integration testing
 * Connects to local ClickHouse instance and creates test tables
 */
export async function setupTestAdapter(): Promise<{
  adapter: ClickHouseAdapter
  cleanup: () => Promise<void>
}> {
  const url = process.env.CLICKHOUSE_URL || 'http://127.0.0.1:8123'
  const database = process.env.CLICKHOUSE_DATABASE || 'payloadtests'
  const namespace = `test_${Date.now()}`
  const embeddingDimensions = 1536

  // Connect without database first to create it
  const bootstrapClient = createClient({
    clickhouse_settings: {
      allow_experimental_json_type: 1,
    },
    password: '',
    url,
    username: 'default',
  })

  await bootstrapClient.command({
    query: `CREATE DATABASE IF NOT EXISTS ${database}`,
  })
  await bootstrapClient.close()

  // Connect to the database
  const client = createClient({
    clickhouse_settings: {
      allow_experimental_json_type: 1,
    },
    database,
    password: '',
    url,
    username: 'default',
  })

  // Create tables
  await client.command({
    query: `
CREATE TABLE IF NOT EXISTS events (
    id String,
    ns String,
    timestamp DateTime64(3),
    type String,
    collection Nullable(String),
    docId Nullable(String),
    userId Nullable(String),
    sessionId Nullable(String),
    ip Nullable(String),
    duration UInt32 DEFAULT 0,
    input JSON,
    result JSON
) ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (ns, timestamp, type)
`,
  })

  await client.command({
    query: `
CREATE TABLE IF NOT EXISTS actions (
    txId String,
    txStatus Enum8('pending' = 0, 'committed' = 1, 'aborted' = 2),
    txTimeout Nullable(DateTime64(3)),
    txCreatedAt DateTime64(3),
    id String,
    ns String,
    type String,
    v DateTime64(3),
    data JSON,
    title String DEFAULT '',
    createdAt DateTime64(3),
    createdBy Nullable(String),
    updatedAt DateTime64(3),
    updatedBy Nullable(String),
    deletedAt Nullable(DateTime64(3)),
    deletedBy Nullable(String)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(txCreatedAt)
ORDER BY (ns, txId, type, id)
`,
  })

  await client.command({
    query: `
CREATE TABLE IF NOT EXISTS search (
    id String,
    ns String,
    collection String,
    docId String,
    chunkIndex UInt16 DEFAULT 0,
    text String,
    embedding Array(Float32),
    status Enum8('pending' = 0, 'ready' = 1, 'failed' = 2),
    errorMessage Nullable(String),
    createdAt DateTime64(3),
    updatedAt DateTime64(3),
    INDEX text_idx text TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4
) ENGINE = ReplacingMergeTree(updatedAt)
PARTITION BY ns
ORDER BY (ns, collection, docId, chunkIndex)
`,
  })

  await client.command({
    query: `
CREATE TABLE IF NOT EXISTS data (
    ns String,
    type String,
    id String,
    v DateTime64(3),
    title String DEFAULT '',
    data JSON,
    createdAt DateTime64(3),
    createdBy Nullable(String),
    updatedAt DateTime64(3),
    updatedBy Nullable(String),
    deletedAt Nullable(DateTime64(3)),
    deletedBy Nullable(String)
) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, type, id, v)
`,
  })

  // Create a minimal adapter object for testing
  const adapter = {
    clickhouse: client,
    defaultTransactionTimeout: 30000,
    embeddingDimensions,
    namespace,
    table: 'data',
  } as unknown as ClickHouseAdapter

  const cleanup = async () => {
    // Clean up test data
    await client.command({
      query: `DELETE FROM events WHERE ns = {ns:String} SETTINGS mutations_sync = 2`,
      query_params: { ns: namespace },
    })
    await client.command({
      query: `DELETE FROM actions WHERE ns = {ns:String} SETTINGS mutations_sync = 2`,
      query_params: { ns: namespace },
    })
    await client.command({
      query: `DELETE FROM search WHERE ns = {ns:String} SETTINGS mutations_sync = 2`,
      query_params: { ns: namespace },
    })
    await client.command({
      query: `DELETE FROM data WHERE ns = {ns:String} SETTINGS mutations_sync = 2`,
      query_params: { ns: namespace },
    })
    await client.close()
  }

  return { adapter, cleanup }
}
