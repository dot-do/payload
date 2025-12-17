import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import type { ClickHouseAdapter } from '../types.js'

import { logEvent } from '../operations/logEvent.js'
import { queryEvents } from '../operations/queryEvents.js'
import { setupTestAdapter } from './helpers/setupTestAdapter.js'

describe.skipIf(!process.env.CLICKHOUSE_TEST)('Events Integration Tests', () => {
  let adapter: ClickHouseAdapter
  let cleanup: () => Promise<void>

  beforeAll(async () => {
    const setup = await setupTestAdapter()
    adapter = setup.adapter
    cleanup = setup.cleanup

    // Bind operations to adapter
    adapter.logEvent = logEvent.bind(adapter)
    adapter.queryEvents = queryEvents.bind(adapter)
  })

  afterAll(async () => {
    await cleanup()
  })

  describe('logEvent', () => {
    it('should log an event and return an id', async () => {
      const eventId = await adapter.logEvent({
        type: 'test.event',
      })

      expect(eventId).toBeDefined()
      expect(eventId).toHaveLength(26) // ULID length
    })

    it('should log an event with all optional fields', async () => {
      const eventId = await adapter.logEvent({
        collection: 'posts',
        docId: 'post-123',
        duration: 100,
        input: { title: 'Test Post' },
        ip: '192.168.1.1',
        result: { success: true },
        sessionId: 'session-abc',
        type: 'doc.create',
        userId: 'user-456',
      })

      expect(eventId).toBeDefined()
      expect(eventId).toHaveLength(26)
    })
  })

  describe('queryEvents', () => {
    it('should query events with default pagination', async () => {
      // Log a few events first
      await adapter.logEvent({ type: 'query.test.1' })
      await adapter.logEvent({ type: 'query.test.2' })
      await adapter.logEvent({ type: 'query.test.3' })

      const result = await adapter.queryEvents({})

      expect(result.docs).toBeDefined()
      expect(Array.isArray(result.docs)).toBe(true)
      expect(result.page).toBe(1)
      expect(result.limit).toBe(10)
      expect(result.totalDocs).toBeGreaterThanOrEqual(3)
    })

    it('should filter events by type', async () => {
      const uniqueType = `unique.type.${Date.now()}`
      await adapter.logEvent({ type: uniqueType })
      await adapter.logEvent({ type: 'other.type' })

      const result = await adapter.queryEvents({
        where: { type: { equals: uniqueType } },
      })

      expect(result.docs.length).toBeGreaterThanOrEqual(1)
      expect(result.docs.every((doc) => doc.type === uniqueType)).toBe(true)
    })

    it('should support pagination', async () => {
      const result = await adapter.queryEvents({
        limit: 2,
        page: 1,
      })

      expect(result.limit).toBe(2)
      expect(result.docs.length).toBeLessThanOrEqual(2)
    })

    it('should sort events by timestamp descending by default', async () => {
      const result = await adapter.queryEvents({
        limit: 5,
      })

      if (result.docs.length >= 2) {
        const timestamps = result.docs.map((doc) => new Date(doc.timestamp as string).getTime())
        for (let i = 1; i < timestamps.length; i++) {
          expect(timestamps[i - 1]).toBeGreaterThanOrEqual(timestamps[i]!)
        }
      }
    })
  })
})
