import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import type { ClickHouseAdapter, DataRow } from '../types.js'

import { generateId, generateVersion } from '../utilities/generateId.js'
import { setupTestAdapter } from './helpers/setupTestAdapter.js'

describe.skipIf(!process.env.CLICKHOUSE_TEST)('Versions Integration Tests', () => {
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
   * Get the versions type name for a collection
   */
  function getVersionsType(collectionSlug: string): string {
    return `_${collectionSlug}_versions`
  }

  /**
   * Helper to insert a version document
   */
  async function insertVersion(data: {
    collectionSlug: string
    parentId: string
    versionData: Record<string, unknown>
    autosave?: boolean
  }) {
    const v = generateVersion()
    const versionType = getVersionsType(data.collectionSlug)

    const versionDoc = {
      parent: data.parentId,
      version: data.versionData,
      ...(data.autosave && { _autosave: true }),
    }

    await adapter.clickhouse!.command({
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
        data: JSON.stringify(versionDoc),
        id: String(v), // Version ID is the v timestamp
        ns: adapter.namespace,
        title: data.versionData.title || String(v),
        type: versionType,
        v,
      },
    })

    return { id: String(v), v }
  }

  /**
   * Helper to query versions
   * Note: With experimental JSON type, we query all versions and filter in JS
   * because JSONExtractString doesn't work directly on JSON type columns
   */
  async function queryVersions(
    collectionSlug: string,
    parentId?: string,
  ): Promise<
    Array<DataRow & { parsedData: { parent: string; version: Record<string, unknown> } }>
  > {
    const versionType = getVersionsType(collectionSlug)

    const result = await adapter.clickhouse!.query({
      format: 'JSONEachRow',
      query: `
        SELECT *
        FROM data FINAL
        WHERE ns = {ns:String} AND type = {type:String} AND deletedAt IS NULL
        ORDER BY v DESC
      `,
      query_params: {
        ns: adapter.namespace,
        type: versionType,
      },
    })

    const rows = await result.json<DataRow>()
    const parsed = rows.map((row) => ({
      ...row,
      parsedData: typeof row.data === 'string' ? JSON.parse(row.data) : row.data,
    }))

    // Filter by parentId in JS if provided
    if (parentId) {
      return parsed.filter((row) => row.parsedData.parent === parentId)
    }

    return parsed
  }

  describe('version storage pattern', () => {
    it('should store versions with type = _${collection}_versions', async () => {
      const collectionSlug = 'posts'
      const parentId = generateId()

      await insertVersion({
        collectionSlug,
        parentId,
        versionData: { content: 'Version 1', title: 'First Version' },
      })

      const versions = await queryVersions(collectionSlug, parentId)

      expect(versions.length).toBe(1)
      expect(versions[0]!.type).toBe('_posts_versions')
    })

    it('should store version ID as v timestamp string', async () => {
      const collectionSlug = 'articles'
      const parentId = generateId()

      const { id, v } = await insertVersion({
        collectionSlug,
        parentId,
        versionData: { title: 'Test Version' },
      })

      expect(id).toBe(String(v))

      const versions = await queryVersions(collectionSlug, parentId)
      expect(versions[0]!.id).toBe(String(v))
    })

    it('should store parent reference in version document', async () => {
      const collectionSlug = 'documents'
      const parentId = generateId()

      await insertVersion({
        collectionSlug,
        parentId,
        versionData: { title: 'Version with parent' },
      })

      const versions = await queryVersions(collectionSlug, parentId)

      expect(versions[0]!.parsedData.parent).toBe(parentId)
    })

    it('should store autosave flag when provided', async () => {
      const collectionSlug = 'drafts'
      const parentId = generateId()

      await insertVersion({
        autosave: true,
        collectionSlug,
        parentId,
        versionData: { title: 'Autosave Version' },
      })

      const versions = await queryVersions(collectionSlug, parentId)

      expect(versions[0]!.parsedData._autosave).toBe(true)
    })
  })

  describe('multiple versions per document', () => {
    it('should store multiple versions for the same parent', async () => {
      const collectionSlug = `multi-ver-${Date.now()}`
      const parentId = generateId()

      // Create 5 versions with sufficient delay to ensure unique timestamps
      for (let i = 1; i <= 5; i++) {
        await insertVersion({
          collectionSlug,
          parentId,
          versionData: { content: `Content v${i}`, title: `Version ${i}`, versionNumber: i },
        })
        await new Promise((resolve) => setTimeout(resolve, 10))
      }

      const versions = await queryVersions(collectionSlug, parentId)

      expect(versions.length).toBe(5)

      // Versions should be ordered by v DESC (newest first)
      expect(versions[0]!.parsedData.version.versionNumber).toBe(5)
      expect(versions[4]!.parsedData.version.versionNumber).toBe(1)
    })

    it('should isolate versions by parent ID', async () => {
      const collectionSlug = 'entries'
      const parentId1 = generateId()
      const parentId2 = generateId()

      // Create versions for first parent
      await insertVersion({
        collectionSlug,
        parentId: parentId1,
        versionData: { owner: 'parent1', title: 'Parent 1 Version' },
      })

      // Create versions for second parent
      await insertVersion({
        collectionSlug,
        parentId: parentId2,
        versionData: { owner: 'parent2', title: 'Parent 2 Version' },
      })

      const versions1 = await queryVersions(collectionSlug, parentId1)
      const versions2 = await queryVersions(collectionSlug, parentId2)

      expect(versions1.length).toBe(1)
      expect(versions2.length).toBe(1)
      expect(versions1[0]!.parsedData.version.owner).toBe('parent1')
      expect(versions2[0]!.parsedData.version.owner).toBe('parent2')
    })
  })

  describe('latest version query pattern', () => {
    it('should find latest version using window function', async () => {
      const collectionSlug = `latest-ver-${Date.now()}`
      const parentId = generateId()

      // Create multiple versions with sufficient delay to ensure unique timestamps
      for (let i = 1; i <= 3; i++) {
        await insertVersion({
          collectionSlug,
          parentId,
          versionData: { title: `Version ${i}` },
        })
        await new Promise((resolve) => setTimeout(resolve, 10))
      }

      // Query all versions and find latest in JS
      // (JSONExtractString doesn't work with experimental JSON type)
      const versions = await queryVersions(collectionSlug, parentId)

      // First result should be the latest (ordered by v DESC)
      expect(versions.length).toBe(3)
      expect(versions[0]!.parsedData.version.title).toBe('Version 3')
    })

    it('should find non-latest versions (exclude latest)', async () => {
      const collectionSlug = 'query-articles'
      const parentId = generateId()

      // Create 4 versions with sufficient delay to ensure unique timestamps
      for (let i = 1; i <= 4; i++) {
        await insertVersion({
          collectionSlug,
          parentId,
          versionData: { title: `Version ${i}`, versionNum: i },
        })
        await new Promise((resolve) => setTimeout(resolve, 10))
      }

      // Query all versions for this parent
      const versions = await queryVersions(collectionSlug, parentId)

      // Skip the first one (latest) to get non-latest versions
      const nonLatestVersions = versions.slice(1)

      // Should have 3 non-latest versions
      expect(nonLatestVersions.length).toBe(3)

      // Check that we got versions 1, 2, 3 (not 4 which is latest)
      const versionNums = nonLatestVersions.map((v) => v.parsedData.version.versionNum).sort()
      expect(versionNums).toEqual([1, 2, 3])
    })
  })

  describe('version soft delete', () => {
    it('should soft delete a version', async () => {
      const collectionSlug = 'pages'
      const parentId = generateId()

      const { id: versionId, v: originalV } = await insertVersion({
        collectionSlug,
        parentId,
        versionData: { title: 'To Be Deleted' },
      })

      // Verify version exists
      let versions = await queryVersions(collectionSlug, parentId)
      expect(versions.length).toBe(1)

      // Soft delete by inserting row with deletedAt
      const deleteV = generateVersion()
      const versionType = getVersionsType(collectionSlug)

      await adapter.clickhouse!.command({
        query: `
          INSERT INTO data (ns, type, id, v, title, data, createdAt, updatedAt, deletedAt)
          VALUES (
            {ns:String}, {type:String}, {id:String},
            fromUnixTimestamp64Milli({v:Int64}),
            'To Be Deleted',
            '${JSON.stringify({ parent: parentId, version: { title: 'To Be Deleted' } })}',
            fromUnixTimestamp64Milli({createdAt:Int64}),
            fromUnixTimestamp64Milli({v:Int64}),
            fromUnixTimestamp64Milli({deletedAt:Int64})
          )
        `,
        query_params: {
          createdAt: originalV,
          deletedAt: deleteV,
          id: versionId,
          ns: adapter.namespace,
          type: versionType,
          v: deleteV,
        },
      })

      // Version should no longer appear
      versions = await queryVersions(collectionSlug, parentId)
      expect(versions.length).toBe(0)
    })
  })

  describe('version data integrity', () => {
    it('should preserve complex version data', async () => {
      const collectionSlug = 'complex-docs'
      const parentId = generateId()

      const complexVersionData = {
        array: [
          { id: 1, name: 'Item 1' },
          { id: 2, name: 'Item 2' },
        ],
        content: {
          blocks: [
            { text: 'Hello', type: 'paragraph' },
            { text: 'World', type: 'heading' },
          ],
        },
        metadata: {
          tags: ['a', 'b', 'c'],
        },
        title: 'Complex Version',
      }

      await insertVersion({
        collectionSlug,
        parentId,
        versionData: complexVersionData,
      })

      const versions = await queryVersions(collectionSlug, parentId)

      expect(versions.length).toBe(1)
      const storedData = versions[0]!.parsedData.version

      expect(storedData.title).toBe('Complex Version')
      expect(storedData.content.blocks).toHaveLength(2)
      expect(storedData.content.blocks[0].type).toBe('paragraph')
      expect(storedData.metadata.tags).toEqual(['a', 'b', 'c'])
      expect(storedData.array).toHaveLength(2)
    })
  })

  describe('namespace isolation for versions', () => {
    it('should isolate versions by namespace', async () => {
      const collectionSlug = 'shared-collection'
      const parentId = generateId()
      const versionType = getVersionsType(collectionSlug)

      // Insert version in our namespace
      await insertVersion({
        collectionSlug,
        parentId,
        versionData: { namespace: 'test', title: 'Test Namespace Version' },
      })

      // Insert version in different namespace
      const v = generateVersion()
      await adapter.clickhouse!.command({
        query: `
          INSERT INTO data (ns, type, id, v, title, data, createdAt, updatedAt)
          VALUES (
            {ns:String}, {type:String}, {id:String},
            fromUnixTimestamp64Milli({v:Int64}),
            'Other Namespace Version',
            {data:String},
            fromUnixTimestamp64Milli({v:Int64}),
            fromUnixTimestamp64Milli({v:Int64})
          )
        `,
        query_params: {
          data: JSON.stringify({
            parent: parentId,
            version: { namespace: 'other', title: 'Other Namespace Version' },
          }),
          id: String(v),
          ns: 'other_namespace',
          type: versionType,
          v,
        },
      })

      // Query should only return our namespace's version
      const versions = await queryVersions(collectionSlug, parentId)

      expect(versions.length).toBe(1)
      expect(versions[0]!.parsedData.version.namespace).toBe('test')
    })
  })
})
