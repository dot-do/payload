import type { ClickHouseClient } from '@clickhouse/client-web'

import { createClient } from '@clickhouse/client-web'

import type { ClickHouseConfig } from '../types.js'

import { getCreateDatabaseSQL, getCreateTableSQL } from './schema.js'

/**
 * Create and initialize ClickHouse client
 */
export async function createClickHouseClient(config: ClickHouseConfig): Promise<ClickHouseClient> {
  const { database = 'default', password = '', table = 'data', url, username = 'default' } = config

  // First connect without specifying database to create it if needed
  const bootstrapClient = createClient({
    clickhouse_settings: {
      allow_experimental_json_type: 1,
    },
    password,
    url,
    username,
  })

  // Create database if it doesn't exist
  await bootstrapClient.command({
    query: getCreateDatabaseSQL(database),
  })

  await bootstrapClient.close()

  // Now connect to the specific database
  const client = createClient({
    clickhouse_settings: {
      allow_experimental_json_type: 1,
    },
    database,
    password,
    url,
    username,
  })

  // Create the data table if it doesn't exist
  await client.command({
    query: getCreateTableSQL(table),
  })

  return client
}

/**
 * Close ClickHouse client connection
 */
export async function closeClickHouseClient(client: ClickHouseClient | null): Promise<void> {
  if (client) {
    await client.close()
  }
}
