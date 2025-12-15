import type { ClickHouseClient } from '@clickhouse/client-web'
import type { DurableObjectState } from '@cloudflare/workers-types'
import type { DrizzleSqliteDODatabase } from 'drizzle-orm/durable-sqlite'

import type { SyncConfig } from '../types.js'

import { syncWithRetry } from '../sync/syncToClickHouse.js'
import { oplogEntriesToClickHouseRows } from '../sync/transform.js'
import {
  cleanupOplog,
  countPendingOplogEntries,
  getPendingOplogEntries,
  markOplogEntriesSynced,
} from './oplog.js'

/**
 * Schedule an alarm for syncing if not already scheduled
 */
export async function scheduleAlarmIfNeeded(
  ctx: DurableObjectState,
  batchWindow: number,
): Promise<void> {
  const currentAlarm = await ctx.storage.getAlarm()
  if (!currentAlarm) {
    // Schedule alarm after batch window (allows grouping of rapid mutations)
    await ctx.storage.setAlarm(Date.now() + batchWindow)
  }
}

/**
 * Handle the alarm - sync pending oplog entries to ClickHouse
 */
export async function handleAlarm(
  ctx: DurableObjectState,
  drizzle: DrizzleSqliteDODatabase<any>,
  clickhouse: ClickHouseClient | null,
  config: {
    namespace: string
    syncConfig: Required<SyncConfig>
    table: string
    tenant: string
  },
): Promise<{ synced: number }> {
  if (!clickhouse) {
    // eslint-disable-next-line no-console
    console.warn('[db-do-clickhouse] ClickHouse client not available, skipping sync')
    return { synced: 0 }
  }

  const { namespace, syncConfig, table, tenant } = config

  // Get pending entries (synchronous)
  const pendingEntries = getPendingOplogEntries(drizzle, syncConfig.batchSize)

  if (pendingEntries.length === 0) {
    return { synced: 0 }
  }

  // Transform to ClickHouse format
  const rows = oplogEntriesToClickHouseRows(pendingEntries, namespace, tenant)

  // Sync to ClickHouse with retry
  const result = await syncWithRetry(clickhouse, table, rows)

  if (result.errors.length > 0) {
    // Log errors but don't throw - we'll retry on next alarm
    // eslint-disable-next-line no-console
    console.error('[db-do-clickhouse] Sync errors:', result.errors)

    // Schedule retry with exponential backoff
    const currentAlarm = await ctx.storage.getAlarm()
    if (!currentAlarm) {
      // Retry after a longer delay
      await ctx.storage.setAlarm(Date.now() + 5000)
    }

    return { synced: 0 }
  }

  // Mark entries as synced (synchronous)
  const maxSeq = Math.max(...pendingEntries.map((e) => e.seq ?? 0))
  markOplogEntriesSynced(drizzle, maxSeq)

  // Check if there are more pending entries (synchronous)
  const remainingCount = countPendingOplogEntries(drizzle)
  if (remainingCount > 0) {
    // Schedule immediate follow-up to process remaining entries
    await ctx.storage.setAlarm(Date.now())
  }

  // Periodically cleanup old synced entries (roughly once per 100 syncs)
  if (Math.random() < 0.01) {
    cleanupOplog(drizzle, syncConfig.retentionDays)
  }

  return { synced: result.synced }
}

/**
 * Force immediate sync (for testing/admin purposes)
 */
export async function forceSync(
  ctx: DurableObjectState,
  drizzle: DrizzleSqliteDODatabase<any>,
  clickhouse: ClickHouseClient | null,
  config: {
    namespace: string
    syncConfig: Required<SyncConfig>
    table: string
    tenant: string
  },
): Promise<{ synced: number }> {
  let totalSynced = 0

  // Keep syncing until all entries are processed
  while (true) {
    const result = await handleAlarm(ctx, drizzle, clickhouse, config)
    totalSynced += result.synced

    if (result.synced === 0) {
      break
    }
  }

  return { synced: totalSynced }
}
