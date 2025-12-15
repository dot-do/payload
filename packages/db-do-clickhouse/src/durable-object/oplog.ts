import type { DrizzleSqliteDODatabase } from 'drizzle-orm/durable-sqlite'

import { sql } from 'drizzle-orm'

import type { OplogEntry } from '../types.js'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type DrizzleDO = DrizzleSqliteDODatabase<any>

/**
 * Initialize the oplog table in SQLite
 * Note: DO SQLite methods are synchronous
 */
export function initOplogTable(drizzle: DrizzleDO): void {
  drizzle.run(sql`
    CREATE TABLE IF NOT EXISTS _oplog (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      op TEXT NOT NULL,
      collection TEXT NOT NULL,
      doc_id TEXT NOT NULL,
      data TEXT,
      timestamp INTEGER NOT NULL,
      synced INTEGER DEFAULT 0
    )
  `)

  // Create index for efficient pending query
  drizzle.run(sql`
    CREATE INDEX IF NOT EXISTS _oplog_pending ON _oplog(synced, seq)
  `)

  // Create index for cleanup queries
  drizzle.run(sql`
    CREATE INDEX IF NOT EXISTS _oplog_cleanup ON _oplog(synced, timestamp)
  `)
}

/**
 * Append an entry to the oplog
 */
export function appendToOplog(
  drizzle: DrizzleDO,
  entry: Omit<OplogEntry, 'seq' | 'synced' | 'timestamp'>,
): void {
  const timestamp = Date.now()
  const dataJson = entry.data ? JSON.stringify(entry.data) : null

  drizzle.run(sql`
    INSERT INTO _oplog (op, collection, doc_id, data, timestamp, synced)
    VALUES (${entry.op}, ${entry.collection}, ${entry.doc_id}, ${dataJson}, ${timestamp}, 0)
  `)
}

/**
 * Get pending oplog entries that need to be synced
 */
export function getPendingOplogEntries(drizzle: DrizzleDO, limit: number = 100): OplogEntry[] {
  const result = drizzle.all<{
    collection: string
    data: null | string
    doc_id: string
    op: string
    seq: number
    synced: number
    timestamp: number
  }>(sql`
    SELECT seq, op, collection, doc_id, data, timestamp, synced
    FROM _oplog
    WHERE synced = 0
    ORDER BY seq ASC
    LIMIT ${limit}
  `)

  return result.map((row) => ({
    collection: row.collection,
    data: row.data ? JSON.parse(row.data) : null,
    doc_id: row.doc_id,
    op: row.op as 'delete' | 'insert' | 'update',
    seq: row.seq,
    synced: row.synced as 0 | 1,
    timestamp: row.timestamp,
  }))
}

/**
 * Mark oplog entries as synced
 */
export function markOplogEntriesSynced(drizzle: DrizzleDO, maxSeq: number): void {
  drizzle.run(sql`
    UPDATE _oplog
    SET synced = 1
    WHERE seq <= ${maxSeq} AND synced = 0
  `)
}

/**
 * Count pending oplog entries
 */
export function countPendingOplogEntries(drizzle: DrizzleDO): number {
  const result = drizzle.get<{ count: number }>(sql`
    SELECT COUNT(*) as count FROM _oplog WHERE synced = 0
  `)
  return result?.count ?? 0
}

/**
 * Cleanup old synced oplog entries
 */
export function cleanupOplog(drizzle: DrizzleDO, retentionDays: number = 7): number {
  const cutoffTimestamp = Date.now() - retentionDays * 24 * 60 * 60 * 1000

  const result = drizzle.run(sql`
    DELETE FROM _oplog
    WHERE synced = 1 AND timestamp < ${cutoffTimestamp}
  `)

  return result.rowsAffected ?? 0
}

/**
 * Get the latest synced timestamp for catch-up queries
 */
export function getLatestSyncedTimestamp(drizzle: DrizzleDO): null | number {
  const result = drizzle.get<{ timestamp: number }>(sql`
    SELECT MAX(timestamp) as timestamp
    FROM _oplog
    WHERE synced = 1
  `)
  return result?.timestamp ?? null
}

/**
 * Get oplog stats for monitoring
 */
export function getOplogStats(drizzle: DrizzleDO): {
  oldestPendingTimestamp: null | number
  pending: number
  synced: number
  total: number
} {
  const stats = drizzle.get<{
    pending: number
    synced: number
    total: number
  }>(sql`
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN synced = 0 THEN 1 ELSE 0 END) as pending,
      SUM(CASE WHEN synced = 1 THEN 1 ELSE 0 END) as synced
    FROM _oplog
  `)

  const oldest = drizzle.get<{ timestamp: number }>(sql`
    SELECT MIN(timestamp) as timestamp
    FROM _oplog
    WHERE synced = 0
  `)

  return {
    oldestPendingTimestamp: oldest?.timestamp ?? null,
    pending: stats?.pending ?? 0,
    synced: stats?.synced ?? 0,
    total: stats?.total ?? 0,
  }
}
