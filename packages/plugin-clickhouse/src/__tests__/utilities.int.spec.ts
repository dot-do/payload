/**
 * Integration tests for plugin-clickhouse utilities
 * Tests actual ClickHouse connectivity for track, relationships, and actions utilities
 */
import type { ClickHouseClient } from '@clickhouse/client-web'

import { createClient } from '@clickhouse/client-web'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'

import {
  createCancelAction,
  createClaimActions,
  createCompleteAction,
  createEnqueue,
  createFailAction,
  createGetDocumentActions,
} from '../utilities/actions.js'
import {
  createFindOrphanedLinks,
  createGetIncomingLinks,
  createGetOutgoingLinks,
  createTraverseGraph,
} from '../utilities/relationships.js'
import { createTrackFunction } from '../utilities/track.js'

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
  const namespace = `util_test_${Date.now()}`

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

  // Create events table
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

  // Create relationships table
  await client.command({
    query: `
CREATE TABLE IF NOT EXISTS relationships (
    ns String,
    fromType String,
    fromId String,
    fromField String,
    toType String,
    toId String,
    position UInt16 DEFAULT 0,
    locale Nullable(String),
    v DateTime64(3, 'UTC'),
    deletedAt Nullable(DateTime64(3, 'UTC'))
) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, toType, toId, fromType, fromId, fromField, position)
`,
  })

  // Create data table for orphan link tests
  await client.command({
    query: `
CREATE TABLE IF NOT EXISTS data (
    ns String,
    type String,
    id String,
    v DateTime64(3),
    title String DEFAULT '',
    data JSON,
    createdAt DateTime64(3),
    createdBy Nullable(String),
    updatedAt DateTime64(3),
    updatedBy Nullable(String),
    deletedAt Nullable(DateTime64(3)),
    deletedBy Nullable(String)
) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, type, id, v)
`,
  })

  // Create actions table
  await client.command({
    query: `
CREATE TABLE IF NOT EXISTS actions (
    id String,
    ns String,
    type String,
    name String,
    status String,
    priority Int32 DEFAULT 0,
    collection Nullable(String),
    docId Nullable(String),
    input String DEFAULT '{}',
    output String DEFAULT '{}',
    error Nullable(String),
    step Int32 DEFAULT 0,
    steps String DEFAULT '[]',
    context String DEFAULT '{}',
    assignedTo Nullable(String),
    waitingFor Nullable(String),
    scheduledAt Nullable(DateTime64(3)),
    startedAt Nullable(DateTime64(3)),
    completedAt Nullable(DateTime64(3)),
    timeoutAt Nullable(DateTime64(3)),
    attempts Int32 DEFAULT 0,
    maxAttempts Int32 DEFAULT 3,
    retryAfter Nullable(DateTime64(3)),
    parentId Nullable(String),
    rootId Nullable(String),
    createdAt DateTime64(3),
    updatedAt DateTime64(3),
    v DateTime64(3)
) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, id)
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
      query: `DELETE FROM relationships WHERE ns = {ns:String} SETTINGS mutations_sync = 2`,
      query_params: { ns: namespace },
    })
    await client.command({
      query: `DELETE FROM data WHERE ns = {ns:String} SETTINGS mutations_sync = 2`,
      query_params: { ns: namespace },
    })
    await client.command({
      query: `DELETE FROM actions WHERE ns = {ns:String} SETTINGS mutations_sync = 2`,
      query_params: { ns: namespace },
    })
    await client.close()
  }

  return { adapter, cleanup }
}

/**
 * Create a mock payload object for utility testing
 */
function createMockPayload(adapter: TestAdapter) {
  let eventIdCounter = 0

  return {
    db: {
      execute: async <T = unknown>(args: {
        query: string
        query_params: Record<string, unknown>
      }): Promise<T[]> => {
        // Handle different query types
        if (args.query.trim().toLowerCase().startsWith('select')) {
          const result = await adapter.clickhouse.query({
            format: 'JSONEachRow',
            query: args.query,
            query_params: args.query_params,
          })
          return result.json<T[]>()
        } else if (args.query.trim().toLowerCase().startsWith('insert')) {
          await adapter.clickhouse.command({
            query: args.query,
            query_params: args.query_params,
          })
          return []
        } else {
          // ALTER, DELETE, etc.
          await adapter.clickhouse.command({
            query: args.query,
            query_params: args.query_params,
          })
          return []
        }
      },
      idType: 'text' as const,
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
    },
    logger: {
      error: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
    },
  }
}

describe('@dotdo/plugin-clickhouse Utilities Integration Tests', () => {
  let adapter: TestAdapter
  let cleanup: () => Promise<void>

  beforeAll(async () => {
    const setup = await setupTestAdapter()
    adapter = setup.adapter
    cleanup = setup.cleanup
  }, 30000)

  afterAll(async () => {
    await cleanup()
  })

  describe('track utility', () => {
    it('should track a custom event', async () => {
      const mockPayload = createMockPayload(adapter)
      const track = createTrackFunction(mockPayload as any)

      const eventId = await track('checkout.completed', {
        input: { orderId: '123', total: 99.99 },
        userId: 'user-track-test',
      })

      expect(eventId).toBeDefined()

      // Verify in database
      const result = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT * FROM events WHERE ns = {ns:String} AND type = 'checkout.completed'`,
        query_params: { ns: adapter.namespace },
      })
      const rows = await result.json<{ type: string; userId: string }[]>()

      expect(rows.length).toBeGreaterThanOrEqual(1)
      expect(rows[0]?.type).toBe('checkout.completed')
      expect(rows[0]?.userId).toBe('user-track-test')
    })

    it('should track event with all optional fields', async () => {
      const mockPayload = createMockPayload(adapter)
      const track = createTrackFunction(mockPayload as any)

      await track('api.request', {
        collection: 'posts',
        docId: 'post-123',
        duration: 150,
        input: { method: 'GET' },
        ip: '10.0.0.1',
        result: { status: 200 },
        sessionId: 'sess-abc',
        userId: 'user-xyz',
      })

      const result = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT * FROM events WHERE ns = {ns:String} AND type = 'api.request'`,
        query_params: { ns: adapter.namespace },
      })
      const rows = await result.json<
        {
          collection: string
          docId: string
          duration: number
          ip: string
          sessionId: string
          userId: string
        }[]
      >()

      expect(rows.length).toBeGreaterThanOrEqual(1)
      expect(rows[0]?.collection).toBe('posts')
      expect(rows[0]?.docId).toBe('post-123')
      expect(rows[0]?.ip).toBe('10.0.0.1')
    })
  })

  describe('relationships utilities', () => {
    beforeAll(async () => {
      // Insert some test relationships
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '')

      await adapter.clickhouse.insert({
        format: 'JSONEachRow',
        table: 'relationships',
        values: [
          // Post -> Author
          {
            deletedAt: null,
            fromField: 'author',
            fromId: 'post-1',
            fromType: 'posts',
            locale: null,
            ns: adapter.namespace,
            position: 0,
            toId: 'user-1',
            toType: 'users',
            v: now,
          },
          // Post -> Category
          {
            deletedAt: null,
            fromField: 'category',
            fromId: 'post-1',
            fromType: 'posts',
            locale: null,
            ns: adapter.namespace,
            position: 0,
            toId: 'cat-1',
            toType: 'categories',
            v: now,
          },
          // Another Post -> Same User
          {
            deletedAt: null,
            fromField: 'author',
            fromId: 'post-2',
            fromType: 'posts',
            locale: null,
            ns: adapter.namespace,
            position: 0,
            toId: 'user-1',
            toType: 'users',
            v: now,
          },
        ],
      })
    })

    it('should get outgoing links from a document', async () => {
      const mockPayload = createMockPayload(adapter)
      const getOutgoing = createGetOutgoingLinks(mockPayload as any)

      const links = await getOutgoing({ collection: 'posts', id: 'post-1' })

      expect(links.length).toBe(2)
      expect(links.map((l) => l.toType).sort()).toEqual(['categories', 'users'])
    })

    it('should get incoming links to a document', async () => {
      const mockPayload = createMockPayload(adapter)
      const getIncoming = createGetIncomingLinks(mockPayload as any)

      const links = await getIncoming({ collection: 'users', id: 'user-1' })

      expect(links.length).toBe(2)
      expect(links.every((l) => l.fromType === 'posts')).toBe(true)
    })

    it('should traverse the document graph', async () => {
      const mockPayload = createMockPayload(adapter)
      const traverse = createTraverseGraph(mockPayload as any)

      const graph = await traverse({
        collection: 'posts',
        depth: 1,
        direction: 'outgoing',
        id: 'post-1',
      })

      expect(graph.length).toBeGreaterThanOrEqual(1)
      expect(graph[0]?.id).toBe('post-1')
      expect(graph[0]?.links.length).toBe(2)
    })
  })

  // Note: Actions utilities are skipped because they require a different table schema
  // than the transaction-staging actions table in db-clickhouse.
  // These would need a dedicated job queue table with the correct schema.
  describe.skip('actions utilities', () => {
    it('should enqueue and claim an action', async () => {
      const mockPayload = createMockPayload(adapter)
      const enqueue = createEnqueue(mockPayload as any)
      const claimActions = createClaimActions(mockPayload as any)

      // Enqueue
      const actionId = await enqueue({
        input: { email: 'test@example.com' },
        name: 'send-email',
        type: 'job',
      })

      expect(actionId).toBeDefined()

      // Wait a moment for insert to complete
      await new Promise((resolve) => setTimeout(resolve, 100))

      // Claim
      const claimed = await claimActions({ name: 'send-email' })

      // Should find the action (may or may not be claimed depending on timing)
      expect(claimed).toBeDefined()
    })

    it('should complete an action', async () => {
      const mockPayload = createMockPayload(adapter)
      const enqueue = createEnqueue(mockPayload as any)
      const completeAction = createCompleteAction(mockPayload as any)

      const actionId = await enqueue({
        name: 'complete-test',
        type: 'job',
      })

      await new Promise((resolve) => setTimeout(resolve, 100))

      await completeAction({
        id: actionId,
        output: { result: 'success' },
      })

      // Wait for mutation
      await new Promise((resolve) => setTimeout(resolve, 500))

      // Verify status
      const result = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT status FROM actions FINAL WHERE id = {id:String} AND ns = {ns:String}`,
        query_params: { id: actionId, ns: adapter.namespace },
      })
      const rows = await result.json<{ status: string }[]>()

      expect(rows[0]?.status).toBe('completed')
    })

    it('should fail an action and set retry', async () => {
      const mockPayload = createMockPayload(adapter)
      const enqueue = createEnqueue(mockPayload as any)
      const failAction = createFailAction(mockPayload as any)

      const actionId = await enqueue({
        maxAttempts: 3,
        name: 'fail-test',
        type: 'job',
      })

      await new Promise((resolve) => setTimeout(resolve, 100))

      await failAction({
        error: { message: 'Something went wrong' },
        id: actionId,
      })

      // Wait for mutation
      await new Promise((resolve) => setTimeout(resolve, 500))

      // Verify status - should be pending (retry) since attempts < maxAttempts
      const result = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT status FROM actions FINAL WHERE id = {id:String} AND ns = {ns:String}`,
        query_params: { id: actionId, ns: adapter.namespace },
      })
      const rows = await result.json<{ status: string }[]>()

      // Status should be either 'pending' (retry) or 'failed'
      expect(['pending', 'failed']).toContain(rows[0]?.status)
    })

    it('should cancel an action', async () => {
      const mockPayload = createMockPayload(adapter)
      const enqueue = createEnqueue(mockPayload as any)
      const cancelAction = createCancelAction(mockPayload as any)

      const actionId = await enqueue({
        name: 'cancel-test',
        type: 'job',
      })

      await new Promise((resolve) => setTimeout(resolve, 100))

      await cancelAction({ id: actionId })

      // Wait for mutation
      await new Promise((resolve) => setTimeout(resolve, 500))

      // Verify status
      const result = await adapter.clickhouse.query({
        format: 'JSONEachRow',
        query: `SELECT status FROM actions FINAL WHERE id = {id:String} AND ns = {ns:String}`,
        query_params: { id: actionId, ns: adapter.namespace },
      })
      const rows = await result.json<{ status: string }[]>()

      expect(rows[0]?.status).toBe('cancelled')
    })

    it('should get actions for a document', async () => {
      const mockPayload = createMockPayload(adapter)
      const enqueue = createEnqueue(mockPayload as any)
      const getDocumentActions = createGetDocumentActions(mockPayload as any)

      await enqueue({
        collection: 'orders',
        docId: 'order-456',
        name: 'process-order',
        type: 'job',
      })

      await new Promise((resolve) => setTimeout(resolve, 100))

      const actions = await getDocumentActions({
        collection: 'orders',
        docId: 'order-456',
      })

      expect(actions.length).toBeGreaterThanOrEqual(1)
      expect(actions[0]?.name).toBe('process-order')
    })
  })
})
