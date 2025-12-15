import type { Connect } from 'payload'

import { createClient } from '@clickhouse/client-web'

import type { ClickHouseAdapter } from './types.js'

/**
 * Validate that a table name is safe to use in SQL
 * Must start with a letter or underscore, followed by letters, numbers, or underscores
 */
function validateTableName(tableName: string): void {
  if (!/^[a-z_]\w*$/i.test(tableName)) {
    throw new Error(
      `Invalid table name '${tableName}'. Table names must start with a letter or underscore and contain only alphanumeric characters and underscores.`,
    )
  }
}

/**
 * Validate that a database name is safe to use in SQL
 * Must start with a letter or underscore, followed by letters, numbers, or underscores
 */
function validateDatabaseName(dbName: string): void {
  if (!/^[a-z_]\w*$/i.test(dbName)) {
    throw new Error(
      `Invalid database name '${dbName}'. Database names must start with a letter or underscore and contain only alphanumeric characters and underscores.`,
    )
  }
}

/**
 * Generate SQL to create the data table if it doesn't exist
 */
function getCreateTableSQL(tableName: string): string {
  validateTableName(tableName)
  return `
CREATE TABLE IF NOT EXISTS ${tableName} (
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
`
}

/**
 * Generate SQL to create the relationships table if it doesn't exist
 */
function getCreateRelationshipsTableSQL(tableName: string): string {
  validateTableName(tableName)
  return `
CREATE TABLE IF NOT EXISTS ${tableName}_relationships (
    ns String,
    fromType String,
    fromId String,
    fromField String,
    toType String,
    toId String,
    position UInt16 DEFAULT 0,
    locale Nullable(String),
    v DateTime64(3, 'UTC'),
    deletedAt Nullable(DateTime64(3, 'UTC'))
) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, toType, toId, fromType, fromId, fromField, position)
`
}

/**
 * Generate SQL to create the actions table if it doesn't exist
 * Actions table is used for transaction staging - writes go here first,
 * then get copied to data table on commit
 */
function getCreateActionsTableSQL(): string {
  return `
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
`
}

/**
 * Generate SQL to create the search table if it doesn't exist
 * Search table supports both full-text and vector similarity search
 */
function getCreateSearchTableSQL(embeddingDimensions: number): string {
  return `
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
    INDEX text_idx text TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4,
    INDEX vec_idx embedding TYPE vector_similarity('hnsw', 'cosineDistance')
) ENGINE = ReplacingMergeTree(updatedAt)
PARTITION BY ns
ORDER BY (ns, collection, docId, chunkIndex)
`
}

/**
 * Connect to ClickHouse and create the database and data table if needed
 */
export const connect: Connect = async function connect(this: ClickHouseAdapter): Promise<void> {
  const { database, password, url, username } = this.config
  const dbName = database || 'default'

  // First connect without specifying database to create it if needed
  const bootstrapClient = createClient({
    clickhouse_settings: {
      allow_experimental_json_type: 1,
    },
    password: password || '',
    url,
    username: username || 'default',
  })

  // Validate database name before using in SQL
  validateDatabaseName(dbName)

  // Create database if it doesn't exist
  try {
    await bootstrapClient.command({
      query: `CREATE DATABASE IF NOT EXISTS ${dbName}`,
    })
  } finally {
    await bootstrapClient.close()
  }

  // Now connect to the specific database
  // Resolve timezone: 'auto' detects from environment, undefined defaults to UTC
  let timezone = this.config.timezone || 'UTC'
  if (timezone === 'auto') {
    try {
      timezone = Intl.DateTimeFormat().resolvedOptions().timeZone
    } catch {
      timezone = 'UTC' // Fallback if Intl is not available
    }
  }
  const client = createClient({
    clickhouse_settings: {
      // Enable JSON type
      allow_experimental_json_type: 1,
      // Set session timezone for consistent DateTime handling
      session_timezone: timezone,
    },
    database: dbName,
    password: password || '',
    url,
    username: username || 'default',
  })

  // Set the clickhouse client for access via payload.db.clickhouse
  this.clickhouse = client

  // Validate and freeze the table name to prevent SQL injection
  // This must be done before any queries use this.table
  validateTableName(this.table)
  Object.defineProperty(this, 'table', {
    configurable: false,
    value: this.table,
    writable: false,
  })

  // Create the data table if it doesn't exist
  await this.clickhouse.command({
    query: getCreateTableSQL(this.table),
  })

  // Create the relationships table if it doesn't exist
  await this.clickhouse.command({
    query: getCreateRelationshipsTableSQL(this.table),
  })

  // Create the actions table if it doesn't exist
  await this.clickhouse.command({
    query: getCreateActionsTableSQL(),
  })

  // Create the search table if it doesn't exist
  await this.clickhouse.command({
    query: getCreateSearchTableSQL(this.embeddingDimensions),
  })

  // Delete data for current namespace if PAYLOAD_DROP_DATABASE is set (for tests)
  if (process.env.PAYLOAD_DROP_DATABASE === 'true') {
    this.payload.logger.info(`---- DROPPING DATA FOR NAMESPACE ${this.namespace} ----`)
    // Use mutations_sync=2 to wait for DELETE mutations to complete on all replicas
    // This ensures test isolation by making deletes synchronous
    await this.clickhouse.command({
      query: `DELETE FROM ${this.table} WHERE ns = {ns:String} SETTINGS mutations_sync = 2`,
      query_params: { ns: this.namespace },
    })
    await this.clickhouse.command({
      query: `DELETE FROM ${this.table}_relationships WHERE ns = {ns:String} SETTINGS mutations_sync = 2`,
      query_params: { ns: this.namespace },
    })
    await this.clickhouse.command({
      query: `DELETE FROM actions WHERE ns = {ns:String} SETTINGS mutations_sync = 2`,
      query_params: { ns: this.namespace },
    })
    await this.clickhouse.command({
      query: `DELETE FROM search WHERE ns = {ns:String} SETTINGS mutations_sync = 2`,
      query_params: { ns: this.namespace },
    })
    this.payload.logger.info('---- DROPPED NAMESPACE DATA ----')
  }
}
