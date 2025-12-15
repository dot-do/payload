import type { DropDatabase } from '@payloadcms/drizzle/sqlite'

import type { DOClickHouseAdapter } from './types.js'

/**
 * Drop all tables in the SQLite database
 * Note: This only affects the local SQLite - ClickHouse data is preserved
 */
export const dropDatabase: DropDatabase = function dropDatabase({ adapter: baseAdapter }) {
  const adapter = baseAdapter as unknown as DOClickHouseAdapter

  // Use the sql.exec() API directly to get tables
  const result = adapter.storage.sql.exec<{ name: string }>(
    `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';`,
  )

  const tables = result.toArray()

  if (tables.length === 0) {
    return
  }

  // Disable foreign key checks to avoid constraint errors during drop
  adapter.storage.sql.exec('PRAGMA foreign_keys = OFF;')

  // Drop each table
  for (const { name } of tables) {
    adapter.storage.sql.exec(`DROP TABLE IF EXISTS "${name}";`)
  }

  // Re-enable foreign key checks
  adapter.storage.sql.exec('PRAGMA foreign_keys = ON;')
}
