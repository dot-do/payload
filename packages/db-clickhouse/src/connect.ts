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

  // Delete data for current namespace if PAYLOAD_DROP_DATABASE is set (for tests)
  if (process.env.PAYLOAD_DROP_DATABASE === 'true') {
    this.payload.logger.info(`---- DROPPING DATA FOR NAMESPACE ${this.namespace} ----`)
    await this.clickhouse.command({
      query: `DELETE FROM ${this.table} WHERE ns = {ns:String}`,
      query_params: { ns: this.namespace },
    })
    this.payload.logger.info('---- DROPPED NAMESPACE DATA ----')
  }
}
