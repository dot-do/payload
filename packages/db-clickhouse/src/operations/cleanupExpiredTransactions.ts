import type { ClickHouseAdapter } from '../types.js'

export interface CleanupExpiredTransactionsResult {
  cleaned: boolean
}

/**
 * Cleans up expired/abandoned transaction records from the actions table.
 * Deletes pending transactions where the timeout has passed.
 * Should be called periodically (e.g., on connect or via scheduled job).
 *
 * @param olderThanMs - Optional grace period in milliseconds. Only delete transactions
 *                      that timed out at least this long ago. Default: 0 (delete immediately after timeout)
 */
export const cleanupExpiredTransactions = async function cleanupExpiredTransactions(
  this: ClickHouseAdapter,
  olderThanMs?: number,
): Promise<CleanupExpiredTransactionsResult> {
  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const now = Date.now()
  const gracePeriod = olderThanMs ?? 0
  const cutoffTime = now - gracePeriod

  // Delete pending transactions that have timed out
  // Only delete if timeout passed before cutoffTime (i.e., timed out at least gracePeriod ago)
  const deleteQuery = `
    ALTER TABLE actions DELETE
    WHERE ns = {ns:String}
      AND type = '_tx_metadata'
      AND txStatus = 'pending'
      AND txTimeout IS NOT NULL
      AND toUnixTimestamp64Milli(txTimeout) < {cutoffTime:Int64}
  `

  await this.clickhouse.command({
    query: deleteQuery,
    query_params: {
      cutoffTime,
      ns: this.namespace,
    },
  })

  return { cleaned: true }
}
