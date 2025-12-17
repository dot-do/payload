import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import type { ClickHouseAdapter, DataRow } from '../types.js'

import { generateId, generateVersion } from '../utilities/generateId.js'
import { setupTestAdapter } from './helpers/setupTestAdapter.js'

describe.skipIf(!process.env.CLICKHOUSE_TEST)('CRUD Integration Tests', () => {
  let adapter: ClickHouseAdapter
  let cleanup: () => Promise<void>

  beforeAll(async () => {
    const setup = await setupTestAdapter()
    adapter = setup.adapter
    cleanup = setup.cleanup
  })

  afterAll(async () => {
    await cleanup()
  })

  /**
   * Helper to parse data field which can be either string or object
   * (experimental JSON type returns object, String type returns string)
   */
  function parseData(data: Record<string, unknown> | string): Record<string, unknown> {
    return typeof data === 'string' ? JSON.parse(data) : data
  }

  /**
   * Helper to insert a document directly into ClickHouse
   */
  async function insertDocument(data: {
    id?: string
    type: string
    data: Record<string, unknown>
    deletedAt?: number | null
  }) {
    const id = data.id || generateId()
    const v = generateVersion()
    const now = Date.now()

    await adapter.clickhouse!.command({
      query: `
        INSERT INTO data (ns, type, id, v, title, data, createdAt, createdBy, updatedAt, updatedBy, deletedAt, deletedBy)
        VALUES (
          {ns:String},
          {type:String},
          {id:String},
          fromUnixTimestamp64Milli({v:Int64}),
          {title:String},
          {data:String},
          fromUnixTimestamp64Milli({createdAt:Int64}),
          NULL,
          fromUnixTimestamp64Milli({updatedAt:Int64}),
          NULL,
          ${data.deletedAt ? 'fromUnixTimestamp64Milli({deletedAt:Int64})' : 'NULL'},
          NULL
        )
      `,
      query_params: {
        createdAt: now,
        data: JSON.stringify(data.data),
        deletedAt: data.deletedAt,
        id,
        ns: adapter.namespace,
        title: data.data.title || id,
        type: data.type,
        updatedAt: now,
        v,
      },
    })

    return { id, v }
  }

  /**
   * Helper to query documents with proper deduplication
   */
  async function queryDocuments(type: string, additionalWhere?: string): Promise<DataRow[]> {
    let whereClause = `ns = {ns:String} AND type = {type:String}`
    if (additionalWhere) {
      whereClause += ` AND ${additionalWhere}`
    }

    const result = await adapter.clickhouse!.query({
      format: 'JSONEachRow',
      query: `
        SELECT * EXCEPT(_rn)
        FROM (
          SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
          FROM data
          WHERE ${whereClause}
        )
        WHERE _rn = 1 AND deletedAt IS NULL
      `,
      query_params: { ns: adapter.namespace, type },
    })

    return result.json<DataRow>()
  }

  describe('ReplacingMergeTree behavior', () => {
    it('should deduplicate rows with same (ns, type, id) by latest v', async () => {
      const type = `test-collection-${Date.now()}`
      const id = generateId()

      // Insert first version
      const v1 = generateVersion()
      await adapter.clickhouse!.command({
        query: `
          INSERT INTO data (ns, type, id, v, title, data, createdAt, updatedAt)
          VALUES (
            {ns:String}, {type:String}, {id:String},
            fromUnixTimestamp64Milli({v:Int64}),
            'Version 1',
            '{"version": 1}',
            fromUnixTimestamp64Milli({v:Int64}),
            fromUnixTimestamp64Milli({v:Int64})
          )
        `,
        query_params: { id, ns: adapter.namespace, type, v: v1 },
      })

      // Insert second version with higher v
      const v2 = generateVersion()
      await adapter.clickhouse!.command({
        query: `
          INSERT INTO data (ns, type, id, v, title, data, createdAt, updatedAt)
          VALUES (
            {ns:String}, {type:String}, {id:String},
            fromUnixTimestamp64Milli({v:Int64}),
            'Version 2',
            '{"version": 2}',
            fromUnixTimestamp64Milli({v:Int64}),
            fromUnixTimestamp64Milli({v:Int64})
          )
        `,
        query_params: { id, ns: adapter.namespace, type, v: v2 },
      })

      // Query with FINAL to force merge
      const result = await adapter.clickhouse!.query({
        format: 'JSONEachRow',
        query: `
          SELECT id, title, data
          FROM data FINAL
          WHERE ns = {ns:String} AND type = {type:String} AND id = {id:String}
        `,
        query_params: { id, ns: adapter.namespace, type },
      })

      const rows = await result.json<{ data: string; id: string; title: string }>()

      // Should have only one row with the latest version
      expect(rows.length).toBe(1)
      expect(rows[0]!.title).toBe('Version 2')
      expect(parseData(rows[0]!.data).version).toBe(2)
    })
  })

  describe('soft delete behavior', () => {
    it('should exclude soft-deleted documents from queries', async () => {
      const type = `test-softdelete-${Date.now()}`

      // Insert active document
      const { id: activeId } = await insertDocument({
        data: { status: 'active', title: 'Active Doc' },
        type,
      })

      // Insert soft-deleted document
      const { id: deletedId } = await insertDocument({
        data: { status: 'deleted', title: 'Deleted Doc' },
        deletedAt: Date.now(),
        type,
      })

      const docs = await queryDocuments(type)

      expect(docs.length).toBe(1)
      expect(docs[0]!.id).toBe(activeId)
      expect(docs.find((d) => d.id === deletedId)).toBeUndefined()
    })

    it('should soft delete by inserting new row with deletedAt', async () => {
      const type = `test-softdelete2-${Date.now()}`

      // Insert active document
      const { id, v: originalV } = await insertDocument({
        data: { title: 'Will Be Deleted' },
        type,
      })

      // Verify document exists
      let docs = await queryDocuments(type)
      expect(docs.length).toBe(1)

      // Soft delete by inserting row with deletedAt
      const deleteV = generateVersion()
      await adapter.clickhouse!.command({
        query: `
          INSERT INTO data (ns, type, id, v, title, data, createdAt, updatedAt, deletedAt)
          VALUES (
            {ns:String}, {type:String}, {id:String},
            fromUnixTimestamp64Milli({v:Int64}),
            'Will Be Deleted',
            '{"title": "Will Be Deleted"}',
            fromUnixTimestamp64Milli({createdAt:Int64}),
            fromUnixTimestamp64Milli({v:Int64}),
            fromUnixTimestamp64Milli({deletedAt:Int64})
          )
        `,
        query_params: {
          createdAt: originalV,
          deletedAt: deleteV,
          id,
          ns: adapter.namespace,
          type,
          v: deleteV,
        },
      })

      // Document should no longer appear in queries
      docs = await queryDocuments(type)
      expect(docs.length).toBe(0)
    })
  })

  describe('namespace isolation', () => {
    it('should isolate documents by namespace', async () => {
      const type = `test-namespace-${Date.now()}`
      const sharedId = generateId()

      // Insert document in our namespace
      await insertDocument({
        data: { namespace: 'test', title: 'Test Namespace Doc' },
        id: sharedId,
        type,
      })

      // Insert document with same id in different namespace
      const v = generateVersion()
      await adapter.clickhouse!.command({
        query: `
          INSERT INTO data (ns, type, id, v, title, data, createdAt, updatedAt)
          VALUES (
            {ns:String}, {type:String}, {id:String},
            fromUnixTimestamp64Milli({v:Int64}),
            'Other Namespace Doc',
            '{"namespace": "other", "title": "Other Namespace Doc"}',
            fromUnixTimestamp64Milli({v:Int64}),
            fromUnixTimestamp64Milli({v:Int64})
          )
        `,
        query_params: { id: sharedId, ns: 'other_namespace', type, v },
      })

      // Query should only return our namespace's document
      const docs = await queryDocuments(type)
      expect(docs.length).toBe(1)
      const data = parseData(docs[0]!.data)
      expect(data.namespace).toBe('test')
    })
  })

  describe('window function deduplication', () => {
    it('should use row_number() to get latest version per document', async () => {
      const type = `test-window-${Date.now()}`
      const id1 = generateId()
      const id2 = generateId()

      // Insert multiple versions of first document
      for (let i = 1; i <= 3; i++) {
        await insertDocument({
          data: { title: `Doc1 v${i}`, version: i },
          id: id1,
          type,
        })
      }

      // Insert multiple versions of second document
      for (let i = 1; i <= 2; i++) {
        await insertDocument({
          data: { title: `Doc2 v${i}`, version: i },
          id: id2,
          type,
        })
      }

      const docs = await queryDocuments(type)

      // Should have exactly 2 documents (latest version of each)
      expect(docs.length).toBe(2)

      const doc1 = docs.find((d) => d.id === id1)
      const doc2 = docs.find((d) => d.id === id2)

      expect(doc1).toBeDefined()
      expect(doc2).toBeDefined()
      expect(parseData(doc1!.data).version).toBe(3)
      expect(parseData(doc2!.data).version).toBe(2)
    })
  })

  describe('JSON data column', () => {
    it('should store and retrieve complex nested JSON', async () => {
      const type = `test-json-${Date.now()}`
      const complexData = {
        array: [1, 2, 3],
        boolean: true,
        nested: {
          deep: {
            value: 'deeply nested',
          },
        },
        nullValue: null,
        number: 42,
        string: 'test',
        title: 'Complex Doc',
      }

      await insertDocument({ data: complexData, type })

      const docs = await queryDocuments(type)

      expect(docs.length).toBe(1)
      const retrieved = parseData(docs[0]!.data)

      expect(retrieved.string).toBe('test')
      expect(retrieved.number).toBe(42)
      expect(retrieved.boolean).toBe(true)
      // Note: ClickHouse JSON type may not preserve null values - they become undefined
      expect(retrieved.nullValue === null || retrieved.nullValue === undefined).toBe(true)
      expect(retrieved.array).toEqual([1, 2, 3])
      expect(retrieved.nested.deep.value).toBe('deeply nested')
    })
  })

  describe('DateTime64 precision', () => {
    it('should preserve millisecond precision in timestamps', async () => {
      const type = `test-datetime-${Date.now()}`
      const v = generateVersion()
      const now = Date.now()

      await adapter.clickhouse!.command({
        query: `
          INSERT INTO data (ns, type, id, v, title, data, createdAt, updatedAt)
          VALUES (
            {ns:String}, {type:String}, {id:String},
            fromUnixTimestamp64Milli({v:Int64}),
            'DateTime Test',
            '{"title": "DateTime Test"}',
            fromUnixTimestamp64Milli({createdAt:Int64}),
            fromUnixTimestamp64Milli({updatedAt:Int64})
          )
        `,
        query_params: {
          createdAt: now,
          id: generateId(),
          ns: adapter.namespace,
          type,
          updatedAt: now,
          v,
        },
      })

      const result = await adapter.clickhouse!.query({
        format: 'JSONEachRow',
        query: `
          SELECT toUnixTimestamp64Milli(createdAt) as createdAtMs,
                 toUnixTimestamp64Milli(updatedAt) as updatedAtMs
          FROM data FINAL
          WHERE ns = {ns:String} AND type = {type:String}
        `,
        query_params: { ns: adapter.namespace, type },
      })

      const rows = await result.json<{ createdAtMs: string; updatedAtMs: string }>()

      expect(rows.length).toBe(1)
      // The stored timestamp should match the original (within 1ms tolerance for any rounding)
      expect(Math.abs(parseInt(rows[0]!.createdAtMs) - now)).toBeLessThanOrEqual(1)
    })
  })

  describe('concurrent updates', () => {
    it('should handle concurrent updates with different versions', async () => {
      const type = `test-concurrent-${Date.now()}`
      const id = generateId()

      // Simulate concurrent updates by inserting with close version timestamps
      const baseV = generateVersion()
      const updates = Array.from({ length: 10 }, (_, i) => ({
        data: { title: `Update ${i}`, updateIndex: i },
        v: baseV + i, // Ensure unique versions
      }))

      // Insert all updates
      await Promise.all(
        updates.map((update) =>
          adapter.clickhouse!.command({
            query: `
              INSERT INTO data (ns, type, id, v, title, data, createdAt, updatedAt)
              VALUES (
                {ns:String}, {type:String}, {id:String},
                fromUnixTimestamp64Milli({v:Int64}),
                {title:String},
                {data:String},
                fromUnixTimestamp64Milli({v:Int64}),
                fromUnixTimestamp64Milli({v:Int64})
              )
            `,
            query_params: {
              data: JSON.stringify(update.data),
              id,
              ns: adapter.namespace,
              title: update.data.title,
              type,
              v: update.v,
            },
          }),
        ),
      )

      const docs = await queryDocuments(type)

      // Should have exactly one document with the highest version
      expect(docs.length).toBe(1)
      const data = parseData(docs[0]!.data)
      expect(data.updateIndex).toBe(9) // Last update
    })
  })
})
