/**
 * Integration tests for plugin-clickhouse hooks
 * Tests actual ClickHouse connectivity and data operations
 */
import type { ClickHouseClient } from '@clickhouse/client-web'

import { createClient } from '@clickhouse/client-web'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'

import { deleteFromSearch } from '../hooks/deleteFromSearch.js'
import { syncWithSearch } from '../hooks/syncWithSearch.js'
import { trackAfterChange, trackAfterDelete } from '../hooks/trackEvent.js'

interface TestAdapter {
  clickhouse: ClickHouseClient
  namespace: string
}

/**
 * Create a minimal test adapter for integration testing
 */
async function setupTestAdapter(): Promise<{
  adapter: TestAdapter
  cleanup: () => Promise<void>
}> {
  const url = process.env.CLICKHOUSE_URL || 'http://127.0.0.1:8123'
  const database = process.env.CLICKHOUSE_DATABASE || 'payloadtests'
  const namespace = `plugin_test_${Date.now()}`

  // Connect without database first to create it
  const bootstrapClient = createClient({
    clickhouse_settings: {
      allow_experimental_json_type: 1,
    },
    password: '',
    url,
    username: 'default',
  })

  await bootstrapClient.command({
    query: `CREATE DATABASE IF NOT EXISTS ${database}`,
  })
  await bootstrapClient.close()

  // Connect to the database
  const client = createClient({
    clickhouse_settings: {
      allow_experimental_json_type: 1,
    },
    database,
    password: '',
    url,
    username: 'default',
  })

  // Create tables needed for plugin tests
  await client.command({
    query: `
CREATE TABLE IF NOT EXISTS events (
    id String,
    ns String,
    timestamp DateTime64(3),
    type String,
    collection Nullable(String),
    docId Nullable(String),
    userId Nullable(String),
    sessionId Nullable(String),
    ip Nullable(String),
    duration UInt32 DEFAULT 0,
    input JSON,
    result JSON
) ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (ns, timestamp, type)
`,
  })

  await client.command({
    query: `
CREATE TABLE IF NOT EXISTS search (
    id String,
    ns String,
    collection String,
    docId String,
    chunkIndex UInt16 DEFAULT 0,
    text String,
    embedding Array(Float32),
    status Enum8('pending' = 0, 'ready' = 1, 'failed' = 2),
    errorMessage Nullable(String),
    createdAt DateTime64(3),
    updatedAt DateTime64(3),
    INDEX text_idx text TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4
) ENGINE = ReplacingMergeTree(updatedAt)
PARTITION BY ns
ORDER BY (ns, collection, docId, chunkIndex)
`,
  })

  const adapter: TestAdapter = {
    clickhouse: client,
    namespace,
  }

  const cleanup = async () => {
    await client.command({
      query: `DELETE FROM events WHERE ns = {ns:String} SETTINGS mutations_sync = 2`,
      query_params: { ns: namespace },
    })
    await client.command({
      query: `DELETE FROM search WHERE ns = {ns:String} SETTINGS mutations_sync = 2`,
      query_params: { ns: namespace },
    })
    await client.close()
  }

  return { adapter, cleanup }
}

/**
 * Create a mock payload object for hook testing
 */
function createMockPayload(adapter: TestAdapter) {
  let eventIdCounter = 0
  let searchIdCounter = 0

  return {
    db: {
      execute: async (args: { query: string; query_params: Record<string, unknown> }) => {
        await adapter.clickhouse.command({
          query: args.query,
          query_params: args.query_params,
        })
      },
      logEvent: async (args: {
        collection?: string
        docId?: string
        duration?: number
        input?: Record<string, unknown>
        ip?: string
        result?: Record<string, unknown>
        sessionId?: string
        type: string
        userId?: string
      }) => {
        const id = `event-${++eventIdCounter}-${Date.now()}`
        const now = Date.now()

        const query = `
          INSERT INTO events (id, ns, timestamp, type, collection, docId, userId, sessionId, ip, duration, input, result)
          VALUES (
            {id:String},
            {ns:String},
            fromUnixTimestamp64Milli({timestamp:Int64}),
            {type:String},
            ${args.collection ? '{collection:String}' : 'NULL'},
            ${args.docId ? '{docId:String}' : 'NULL'},
            ${args.userId ? '{userId:String}' : 'NULL'},
            ${args.sessionId ? '{sessionId:String}' : 'NULL'},
            ${args.ip ? '{ip:String}' : 'NULL'},
            {duration:UInt32},
            {input:String},
            {result:String}
          )
        `

        const params: Record<string, unknown> = {
          duration: args.duration || 0,
          id,
          input: JSON.stringify(args.input || {}),
          ns: adapter.namespace,
          result: JSON.stringify(args.result || {}),
          timestamp: now,
          type: args.type,
        }

        if (args.collection) params.collection = args.collection
        if (args.docId) params.docId = args.docId
        if (args.userId) params.userId = args.userId
        if (args.sessionId) params.sessionId = args.sessionId
        if (args.ip) params.ip = args.ip

        await adapter.clickhouse.command({
          query,
          query_params: params,
        })
        return id
      },
      namespace: adapter.namespace,
      syncToSearch: async (args: {
        chunkIndex: number
        collection: string
        doc: Record<string, unknown>
      }) => {
        const id = `search-${++searchIdCounter}-${Date.now()}`
        const now = new Date().toISOString().replace('T', ' ').replace('Z', '')
        await adapter.clickhouse.insert({
          format: 'JSONEachRow',
          table: 'search',
          values: [
            {
              chunkIndex: args.chunkIndex,
              collection: args.collection,
              createdAt: now,
              docId: String(args.doc.id),
              embedding: [],
              errorMessage: null,
              id,
              ns: adapter.namespace,
              status: 'pending',
              text: (args.doc._extractedText as string) || '',
              updatedAt: now,
            },
          ],
        })
        return id
      },
    },
    logger: {
      error: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
    },
  }
}

/**
 * Create a mock request object
 */
function createMockRequest(
  payload: ReturnType<typeof createMockPayload>,
  options?: { user?: { id: string } },
) {
  return {
    headers: {
      get: (name: string) => {
        if (name === 'x-forwarded-for') return '192.168.1.100'
        if (name === 'x-session-id') return 'test-session-123'
        return null
      },
    },
    payload,
    user: options?.user,
  }
}

describe('@dotdo/plugin-clickhouse Integration Tests', () => {
  let adapter: TestAdapter
  let cleanup: () => Promise<void>

  beforeAll(async () => {
    const setup = await setupTestAdapter()
    adapter = setup.adapter
    cleanup = setup.cleanup
  }, 30000) // 30 second timeout for setup

  afterAll(async () => {
    await cleanup()
  })

  describe('syncWithSearch hook', () => {
    it('should sync document to search table', async () => {
      const mockPayload = createMockPayload(adapter)
      const mockReq = createMockRequest(mockPayload)

      const hook = syncWithSearch({
        collectionSlug: 'posts',
        searchConfig: { fields: ['title', 'content'] },
      })

      const doc = {
        content: 'This is the full content of the test post.',
        id: 'post-123',
        title: 'Test Post Title',
      }

      await hook({
        context: {} as any,
        doc,
        operation: 'create',
        previousDoc: undefined,
        req: mockReq as any,
      })

      // Query the search table to verify the sync
      const result = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT * FROM search WHERE ns = {ns:String} AND docId = {docId:String}`,
        query_params: { docId: 'post-123', ns: adapter.namespace },
      })
      const rows = await result.json<{ collection: string; docId: string; text: string }[]>()

      expect(rows.length).toBeGreaterThanOrEqual(1)
      expect(rows[0]?.collection).toBe('posts')
      expect(rows[0]?.docId).toBe('post-123')
      expect(rows[0]?.text).toContain('Test Post Title')
    })

    it('should chunk large text content', async () => {
      const mockPayload = createMockPayload(adapter)
      const mockReq = createMockRequest(mockPayload)

      const hook = syncWithSearch({
        chunkOverlap: 50,
        chunkSize: 200,
        collectionSlug: 'articles',
        searchConfig: { fields: ['content'] },
      })

      // Create content larger than chunk size
      const longContent = 'A'.repeat(500)
      const doc = {
        content: longContent,
        id: 'article-456',
        title: 'Long Article',
      }

      await hook({
        context: {} as any,
        doc,
        operation: 'create',
        previousDoc: undefined,
        req: mockReq as any,
      })

      // Query the search table
      const result = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT chunkIndex FROM search WHERE ns = {ns:String} AND docId = {docId:String} ORDER BY chunkIndex`,
        query_params: { docId: 'article-456', ns: adapter.namespace },
      })
      const rows = await result.json<{ chunkIndex: number }[]>()

      // Should have multiple chunks
      expect(rows.length).toBeGreaterThan(1)
    })

    it('should skip sync when no text extracted', async () => {
      const mockPayload = createMockPayload(adapter)
      const mockReq = createMockRequest(mockPayload)

      const hook = syncWithSearch({
        collectionSlug: 'empty-docs',
        searchConfig: { fields: ['nonexistent'] },
      })

      const doc = {
        id: 'empty-789',
        other: 'field',
      }

      const result = await hook({
        context: {} as any,
        doc,
        operation: 'create',
        previousDoc: undefined,
        req: mockReq as any,
      })

      expect(result).toBe(doc)

      // Verify nothing was inserted
      const queryResult = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT * FROM search WHERE ns = {ns:String} AND docId = {docId:String}`,
        query_params: { docId: 'empty-789', ns: adapter.namespace },
      })
      const rows = await queryResult.json()

      expect(rows.length).toBe(0)
    })
  })

  describe('deleteFromSearch hook', () => {
    it('should delete document from search table', async () => {
      const mockPayload = createMockPayload(adapter)
      const mockReq = createMockRequest(mockPayload)

      // First insert a search entry
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '')
      await adapter.clickhouse.insert({
        format: 'JSONEachRow',
        table: 'search',
        values: [
          {
            chunkIndex: 0,
            collection: 'posts',
            createdAt: now,
            docId: 'delete-test-123',
            embedding: [],
            id: 'search-to-delete',
            ns: adapter.namespace,
            status: 'ready',
            text: 'Text to delete',
            updatedAt: now,
          },
        ],
      })

      // Verify it exists
      let result = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT * FROM search WHERE ns = {ns:String} AND docId = {docId:String}`,
        query_params: { docId: 'delete-test-123', ns: adapter.namespace },
      })
      let rows = await result.json()
      expect(rows.length).toBe(1)

      // Run the delete hook
      const hook = deleteFromSearch({ collectionSlug: 'posts' })
      await hook({
        context: {} as any,
        id: 'delete-test-123',
        req: mockReq as any,
      })

      // Wait for mutation to complete (ClickHouse mutations are async)
      await new Promise((resolve) => setTimeout(resolve, 1000))

      // Verify it was deleted
      result = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT * FROM search FINAL WHERE ns = {ns:String} AND docId = {docId:String}`,
        query_params: { docId: 'delete-test-123', ns: adapter.namespace },
      })
      rows = await result.json()
      expect(rows.length).toBe(0)
    })
  })

  describe('trackAfterChange hook', () => {
    it('should log create event', async () => {
      const mockPayload = createMockPayload(adapter)
      const mockReq = createMockRequest(mockPayload, { user: { id: 'user-456' } })

      const hook = trackAfterChange({
        collectionSlug: 'posts',
      })

      const doc = { id: 'create-event-test' }
      await hook({
        context: {} as any,
        doc,
        operation: 'create',
        previousDoc: undefined,
        req: mockReq as any,
      })

      // Query events table
      const result = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT * FROM events WHERE ns = {ns:String} AND type = 'doc.create' AND docId = {docId:String}`,
        query_params: { docId: 'create-event-test', ns: adapter.namespace },
      })
      const rows = await result.json<{ collection: string; type: string; userId: string }[]>()

      expect(rows.length).toBeGreaterThanOrEqual(1)
      expect(rows[0]?.type).toBe('doc.create')
      expect(rows[0]?.collection).toBe('posts')
      expect(rows[0]?.userId).toBe('user-456')
    })

    it('should log update event', async () => {
      const mockPayload = createMockPayload(adapter)
      const mockReq = createMockRequest(mockPayload)

      const hook = trackAfterChange({
        collectionSlug: 'posts',
      })

      const doc = { id: 'update-event-test' }
      await hook({
        context: {} as any,
        doc,
        operation: 'update',
        previousDoc: { id: 'update-event-test', title: 'Old' },
        req: mockReq as any,
      })

      // Query events table
      const result = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT * FROM events WHERE ns = {ns:String} AND type = 'doc.update' AND docId = {docId:String}`,
        query_params: { docId: 'update-event-test', ns: adapter.namespace },
      })
      const rows = await result.json<{ type: string }[]>()

      expect(rows.length).toBeGreaterThanOrEqual(1)
      expect(rows[0]?.type).toBe('doc.update')
    })

    it('should capture IP and session from headers', async () => {
      const mockPayload = createMockPayload(adapter)
      const mockReq = createMockRequest(mockPayload)

      const hook = trackAfterChange({
        collectionSlug: 'posts',
      })

      const doc = { id: 'headers-event-test' }
      await hook({
        context: {} as any,
        doc,
        operation: 'create',
        previousDoc: undefined,
        req: mockReq as any,
      })

      // Query events table
      const result = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT ip, sessionId FROM events WHERE ns = {ns:String} AND docId = {docId:String}`,
        query_params: { docId: 'headers-event-test', ns: adapter.namespace },
      })
      const rows = await result.json<{ ip: string; sessionId: string }[]>()

      expect(rows.length).toBeGreaterThanOrEqual(1)
      expect(rows[0]?.ip).toBe('192.168.1.100')
      expect(rows[0]?.sessionId).toBe('test-session-123')
    })
  })

  describe('trackAfterDelete hook', () => {
    it('should log delete event', async () => {
      const mockPayload = createMockPayload(adapter)
      const mockReq = createMockRequest(mockPayload, { user: { id: 'user-789' } })

      const hook = trackAfterDelete({
        collectionSlug: 'posts',
      })

      await hook({
        context: {} as any,
        doc: { id: 'delete-event-test' },
        id: 'delete-event-test',
        req: mockReq as any,
      })

      // Query events table
      const result = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT * FROM events WHERE ns = {ns:String} AND type = 'doc.delete' AND docId = {docId:String}`,
        query_params: { docId: 'delete-event-test', ns: adapter.namespace },
      })
      const rows = await result.json<{ collection: string; type: string; userId: string }[]>()

      expect(rows.length).toBeGreaterThanOrEqual(1)
      expect(rows[0]?.type).toBe('doc.delete')
      expect(rows[0]?.collection).toBe('posts')
      expect(rows[0]?.userId).toBe('user-789')
    })
  })
})
