import type { DropDatabase } from '@payloadcms/drizzle/sqlite'

import type { SQLiteDOAdapter } from './types.js'

export const dropDatabase: DropDatabase = function ({ adapter: baseAdapter }) {
  const adapter = baseAdapter as unknown as SQLiteDOAdapter
  // Use the sql.exec() API to get tables
  const result = adapter.storage.sql.exec<{ name: string }>(
    `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';`,
  )

  const tables = result.toArray()

  if (tables.length === 0) {
    return
  }

  // Drop each table
  adapter.storage.sql.exec('PRAGMA foreign_keys = OFF;')

  for (const { name } of tables) {
    adapter.storage.sql.exec(`DROP TABLE IF EXISTS "${name}";`)
  }

  adapter.storage.sql.exec('PRAGMA foreign_keys = ON;')
}
