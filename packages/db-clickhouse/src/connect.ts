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
 * Connect to ClickHouse and create the data table if needed
 */
export const connect: Connect = async function connect(this: ClickHouseAdapter): Promise<void> {
  const { database, password, url, username } = this.config

  this.client = createClient({
    clickhouse_settings: {
      // Enable JSON type
      allow_experimental_json_type: 1,
      // Use FINAL by default for consistent reads
      // final: 1, // This might be too aggressive, let queries decide
    },
    database: database || 'default',
    password: password || '',
    url,
    username: username || 'default',
  })

  // Create the data table if it doesn't exist
  await this.client.command({
    query: getCreateTableSQL(this.table),
  })
}
