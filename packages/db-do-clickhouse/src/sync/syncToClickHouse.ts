import type { ClickHouseClient } from '@clickhouse/client-web'

import type { ClickHouseInsertRow } from './transform.js'

/**
 * Result of a sync operation
 */
export interface SyncResult {
  errors: Error[]
  synced: number
}

/**
 * Sync rows to ClickHouse using batch insert
 */
export async function syncRowsToClickHouse(
  client: ClickHouseClient,
  table: string,
  rows: ClickHouseInsertRow[],
): Promise<SyncResult> {
  if (rows.length === 0) {
    return { errors: [], synced: 0 }
  }

  const errors: Error[] = []

  try {
    // Use JSONEachRow format for efficient batch insert
    await client.insert({
      format: 'JSONEachRow',
      table,
      values: rows.map((row) => ({
        id: row.id,
        type: row.type,
        createdAt: row.createdAt,
        createdBy: row.createdBy,
        data: row.data,
        deletedAt: row.deletedAt,
        deletedBy: row.deletedBy,
        ns: row.ns,
        tenant: row.tenant,
        title: row.title,
        updatedAt: row.updatedAt,
        updatedBy: row.updatedBy,
        v: row.v,
      })),
    })

    return { errors: [], synced: rows.length }
  } catch (error) {
    errors.push(error instanceof Error ? error : new Error(String(error)))
    return { errors, synced: 0 }
  }
}

/**
 * Retry configuration for sync operations
 */
export interface RetryConfig {
  initialDelayMs: number
  maxAttempts: number
  maxDelayMs: number
  multiplier: number
}

const defaultRetryConfig: RetryConfig = {
  initialDelayMs: 100,
  maxAttempts: 5,
  maxDelayMs: 30000,
  multiplier: 2,
}

/**
 * Calculate delay for exponential backoff
 */
export function calculateBackoffDelay(
  attempt: number,
  config: RetryConfig = defaultRetryConfig,
): number {
  const delay = config.initialDelayMs * Math.pow(config.multiplier, attempt)
  return Math.min(delay, config.maxDelayMs)
}

/**
 * Sync with retry logic
 */
export async function syncWithRetry(
  client: ClickHouseClient,
  table: string,
  rows: ClickHouseInsertRow[],
  config: RetryConfig = defaultRetryConfig,
): Promise<SyncResult> {
  let lastError: Error | null = null

  for (let attempt = 0; attempt < config.maxAttempts; attempt++) {
    const result = await syncRowsToClickHouse(client, table, rows)

    if (result.errors.length === 0) {
      return result
    }

    lastError = result.errors[0] ?? new Error('Unknown sync error')

    // Don't sleep on the last attempt
    if (attempt < config.maxAttempts - 1) {
      const delay = calculateBackoffDelay(attempt, config)
      await new Promise((resolve) => setTimeout(resolve, delay))
    }
  }

  return {
    errors: lastError ? [lastError] : [new Error('Max retries exceeded')],
    synced: 0,
  }
}
