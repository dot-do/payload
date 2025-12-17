import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import type { ClickHouseAdapter } from '../types.js'

import { beginTransaction } from '../operations/beginTransaction.js'
import { commitTransaction } from '../operations/commitTransaction.js'
import { rollbackTransaction } from '../operations/rollbackTransaction.js'
import { setupTestAdapter } from './helpers/setupTestAdapter.js'

describe.skipIf(!process.env.CLICKHOUSE_TEST)('Transactions Integration Tests', () => {
  let adapter: ClickHouseAdapter
  let cleanup: () => Promise<void>

  beforeAll(async () => {
    const setup = await setupTestAdapter()
    adapter = setup.adapter
    cleanup = setup.cleanup

    // Bind operations to adapter
    adapter.beginTransaction = beginTransaction.bind(adapter)
    adapter.commitTransaction = commitTransaction.bind(adapter)
    adapter.rollbackTransaction = rollbackTransaction.bind(adapter)
  })

  afterAll(async () => {
    await cleanup()
  })

  describe('beginTransaction', () => {
    it('should start a transaction and return a txId', async () => {
      const txId = await adapter.beginTransaction({})

      expect(txId).toBeDefined()
      expect(typeof txId).toBe('string')
      expect(txId.length).toBeGreaterThan(0)
    })

    it('should create a transaction with custom timeout', async () => {
      const txId = await adapter.beginTransaction({ timeout: 60000 })

      expect(txId).toBeDefined()
    })

    it('should create a transaction with null timeout (no expiry)', async () => {
      const txId = await adapter.beginTransaction({ timeout: null })

      expect(txId).toBeDefined()
    })
  })

  describe('commitTransaction', () => {
    it('should commit a transaction', async () => {
      const txId = await adapter.beginTransaction({})

      // Should not throw
      await adapter.commitTransaction(txId as string)

      // Verify transaction was marked as committed by checking actions table
      const result = await adapter.clickhouse!.query({
        format: 'JSONEachRow',
        query: `SELECT txStatus FROM actions WHERE txId = {txId:String} AND type = '_tx_metadata' ORDER BY txCreatedAt DESC LIMIT 1`,
        query_params: { txId },
      })
      const rows = await result.json<{ txStatus: string }[]>()

      expect(rows.length).toBe(1)
      expect(rows[0]?.txStatus).toBe('committed')
    })

    it('should handle null/empty txId gracefully', async () => {
      // Should not throw
      await adapter.commitTransaction('')
    })
  })

  describe('rollbackTransaction', () => {
    it('should rollback a transaction', async () => {
      const txId = await adapter.beginTransaction({})

      // Should not throw
      await adapter.rollbackTransaction(txId as string)

      // Verify transaction was marked as aborted
      const result = await adapter.clickhouse!.query({
        format: 'JSONEachRow',
        query: `SELECT txStatus FROM actions WHERE txId = {txId:String} AND type = '_tx_metadata' ORDER BY txCreatedAt DESC LIMIT 1`,
        query_params: { txId },
      })
      const rows = await result.json<{ txStatus: string }[]>()

      expect(rows.length).toBe(1)
      expect(rows[0]?.txStatus).toBe('aborted')
    })

    it('should handle null/empty txId gracefully', async () => {
      // Should not throw
      await adapter.rollbackTransaction('')
    })
  })
})
