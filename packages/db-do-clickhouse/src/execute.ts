import type { Execute } from '@payloadcms/drizzle'

import { sql } from 'drizzle-orm'

/**
 * Execute function for Durable Objects SQLite
 *
 * DO SQLite uses synchronous execution via storage.sql.exec()
 * The Drizzle DO driver wraps this but uses different methods than D1
 */
export const execute: Execute<any> = function execute({ db, drizzle, raw, sql: statement }): any {
  const executeFrom: any = (db ?? drizzle)!

  // Create a result object compatible with what Payload expects
  const createResult = (rows: any[]) => ({
    columns: undefined,
    columnTypes: undefined,
    lastInsertRowid: BigInt(0),
    rows,
    rowsAffected: 0,
  })

  // For DO SQLite, we need to use .all() to get results
  if (raw) {
    const query = sql.raw(raw)
    const rows = executeFrom.all(query)
    return createResult(rows)
  } else if (statement) {
    const rows = executeFrom.all(statement)
    return createResult(rows)
  }

  return createResult([])
}
