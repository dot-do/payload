import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import type { ClickHouseAdapter } from '../types.js'

import { getSearchQueue } from '../operations/getSearchQueue.js'
import { search } from '../operations/search.js'
import { syncToSearch } from '../operations/syncToSearch.js'
import { updateSearchStatus } from '../operations/updateSearchStatus.js'
import { setupTestAdapter } from './helpers/setupTestAdapter.js'

describe.skipIf(!process.env.CLICKHOUSE_TEST)('Search Integration Tests', () => {
  let adapter: ClickHouseAdapter
  let cleanup: () => Promise<void>

  beforeAll(async () => {
    const setup = await setupTestAdapter()
    adapter = setup.adapter
    cleanup = setup.cleanup

    // Bind operations to adapter
    adapter.syncToSearch = syncToSearch.bind(adapter)
    adapter.search = search.bind(adapter)
    adapter.getSearchQueue = getSearchQueue.bind(adapter)
    adapter.updateSearchStatus = updateSearchStatus.bind(adapter)
  })

  afterAll(async () => {
    await cleanup()
  })

  describe('syncToSearch', () => {
    it('should sync a document to the search table', async () => {
      const searchId = await adapter.syncToSearch({
        collection: 'posts',
        doc: {
          content: 'This is the post content',
          id: 'post-123',
          title: 'Test Post',
        },
      })

      expect(searchId).toBeDefined()
      expect(searchId).toHaveLength(26) // ULID length
    })

    it('should sync a document with chunk index', async () => {
      const searchId = await adapter.syncToSearch({
        chunkIndex: 1,
        collection: 'articles',
        doc: {
          content: 'Chunk 1 of the article',
          id: 'article-456',
          title: 'Long Article',
        },
      })

      expect(searchId).toBeDefined()
    })
  })

  describe('getSearchQueue', () => {
    it('should return pending search items', async () => {
      // Sync a document first (creates pending item)
      await adapter.syncToSearch({
        collection: 'queue-test',
        doc: {
          id: `doc-${Date.now()}`,
          title: 'Queue Test',
        },
      })

      const queue = await adapter.getSearchQueue({})

      expect(Array.isArray(queue)).toBe(true)
      // Should have at least one pending item
      expect(queue.length).toBeGreaterThanOrEqual(1)
    })

    it('should respect limit parameter', async () => {
      const queue = await adapter.getSearchQueue({ limit: 1 })

      expect(queue.length).toBeLessThanOrEqual(1)
    })

    it('should return items with expected fields', async () => {
      await adapter.syncToSearch({
        collection: 'field-test',
        doc: {
          id: `doc-${Date.now()}`,
          title: 'Field Test',
        },
      })

      const queue = await adapter.getSearchQueue({ limit: 1 })

      if (queue.length > 0) {
        const item = queue[0]!
        expect(item).toHaveProperty('id')
        expect(item).toHaveProperty('collection')
        expect(item).toHaveProperty('docId')
        expect(item).toHaveProperty('text')
      }
    })
  })

  describe('updateSearchStatus', () => {
    it('should update status to ready with embedding', async () => {
      // Create a pending search item
      const searchId = await adapter.syncToSearch({
        collection: 'status-test',
        doc: {
          id: `doc-${Date.now()}`,
          title: 'Status Test',
        },
      })

      // Create a mock embedding
      const embedding = new Array(adapter.embeddingDimensions).fill(0).map(() => Math.random())

      // Update status
      await adapter.updateSearchStatus({
        embedding,
        id: searchId,
        status: 'ready',
      })

      // Verify the update
      const result = await adapter.clickhouse!.query({
        format: 'JSONEachRow',
        query: `SELECT status FROM search WHERE id = {id:String} ORDER BY updatedAt DESC LIMIT 1`,
        query_params: { id: searchId },
      })
      const rows = await result.json<{ status: string }[]>()

      expect(rows.length).toBe(1)
      expect(rows[0]?.status).toBe('ready')
    })

    it('should update status to failed with error message', async () => {
      const searchId = await adapter.syncToSearch({
        collection: 'fail-test',
        doc: {
          id: `doc-${Date.now()}`,
          title: 'Fail Test',
        },
      })

      await adapter.updateSearchStatus({
        error: 'Embedding generation failed',
        id: searchId,
        status: 'failed',
      })

      // Verify the update
      const result = await adapter.clickhouse!.query({
        format: 'JSONEachRow',
        query: `SELECT status, errorMessage FROM search WHERE id = {id:String} ORDER BY updatedAt DESC LIMIT 1`,
        query_params: { id: searchId },
      })
      const rows = await result.json<{ errorMessage: string; status: string }[]>()

      expect(rows.length).toBe(1)
      expect(rows[0]?.status).toBe('failed')
      expect(rows[0]?.errorMessage).toBe('Embedding generation failed')
    })
  })

  describe('search', () => {
    it('should perform text search', async () => {
      const uniqueTitle = `unique-search-${Date.now()}`

      // Create and make ready a search item
      const searchId = await adapter.syncToSearch({
        collection: 'text-search',
        doc: {
          id: `doc-${Date.now()}`,
          title: uniqueTitle,
        },
      })

      // Update to ready status with embedding
      const embedding = new Array(adapter.embeddingDimensions).fill(0.1)
      await adapter.updateSearchStatus({
        embedding,
        id: searchId,
        status: 'ready',
      })

      // Search for the document
      const result = await adapter.search({
        text: uniqueTitle,
      })

      expect(result.docs).toBeDefined()
      expect(Array.isArray(result.docs)).toBe(true)
    })

    it('should filter by collection', async () => {
      const collection = `filter-test-${Date.now()}`

      const searchId = await adapter.syncToSearch({
        collection,
        doc: {
          id: `doc-${Date.now()}`,
          title: 'Filter Test',
        },
      })

      const embedding = new Array(adapter.embeddingDimensions).fill(0.1)
      await adapter.updateSearchStatus({
        embedding,
        id: searchId,
        status: 'ready',
      })

      const result = await adapter.search({
        where: { collection: { equals: collection } },
      })

      expect(result.docs.every((doc) => doc.collection === collection)).toBe(true)
    })

    it('should support vector search', async () => {
      const searchId = await adapter.syncToSearch({
        collection: 'vector-search',
        doc: {
          id: `doc-${Date.now()}`,
          title: 'Vector Test',
        },
      })

      const embedding = new Array(adapter.embeddingDimensions).fill(0.5)
      await adapter.updateSearchStatus({
        embedding,
        id: searchId,
        status: 'ready',
      })

      // Search with similar vector
      const queryVector = new Array(adapter.embeddingDimensions).fill(0.5)
      const result = await adapter.search({
        vector: queryVector,
      })

      expect(result.docs).toBeDefined()
    })
  })
})
