import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import type { ClickHouseAdapter } from '../types.js'

import { generateId, generateVersion } from '../utilities/generateId.js'
import {
  insertRelationships,
  softDeleteRelationships,
  softDeleteRelationshipsToDocument,
} from '../utilities/relationships.js'
import { setupTestAdapter } from './helpers/setupTestAdapter.js'

describe.skipIf(!process.env.CLICKHOUSE_TEST)('Relationships Integration Tests', () => {
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

  // Helper to query active relationships
  async function queryActiveRelationships(
    fromType?: string,
    fromId?: string,
    toType?: string,
    toId?: string,
  ) {
    let whereClause = `ns = {ns:String}`
    const params: Record<string, string> = { ns: adapter.namespace }

    if (fromType) {
      whereClause += ` AND fromType = {fromType:String}`
      params.fromType = fromType
    }
    if (fromId) {
      whereClause += ` AND fromId = {fromId:String}`
      params.fromId = fromId
    }
    if (toType) {
      whereClause += ` AND toType = {toType:String}`
      params.toType = toType
    }
    if (toId) {
      whereClause += ` AND toId = {toId:String}`
      params.toId = toId
    }

    const result = await adapter.clickhouse!.query({
      format: 'JSONEachRow',
      query: `
        SELECT fromType, fromId, fromField, toType, toId, position
        FROM (
          SELECT *,
            row_number() OVER (
              PARTITION BY ns, fromType, fromId, fromField, toType, toId, position
              ORDER BY v DESC
            ) as _rn
          FROM data_relationships
          WHERE ${whereClause}
        )
        WHERE _rn = 1 AND deletedAt IS NULL
      `,
      query_params: params,
    })

    return result.json<{
      fromField: string
      fromId: string
      fromType: string
      position: number
      toId: string
      toType: string
    }>()
  }

  describe('insertRelationships', () => {
    it('should insert relationship rows', async () => {
      const v = generateVersion()
      const fromId = generateId()
      const toId = generateId()

      await insertRelationships(adapter.clickhouse!, adapter.table, [
        {
          deletedAt: null,
          fromField: 'author',
          fromId,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId,
          toType: 'users',
          v,
        },
      ])

      const relationships = await queryActiveRelationships('posts', fromId)

      expect(relationships.length).toBe(1)
      expect(relationships[0]).toMatchObject({
        fromField: 'author',
        fromId,
        fromType: 'posts',
        toId,
        toType: 'users',
      })
    })

    it('should insert multiple relationships with positions', async () => {
      const v = generateVersion()
      const fromId = generateId()
      const toId1 = generateId()
      const toId2 = generateId()
      const toId3 = generateId()

      await insertRelationships(adapter.clickhouse!, adapter.table, [
        {
          deletedAt: null,
          fromField: 'tags',
          fromId,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: toId1,
          toType: 'tags',
          v,
        },
        {
          deletedAt: null,
          fromField: 'tags',
          fromId,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 1,
          toId: toId2,
          toType: 'tags',
          v,
        },
        {
          deletedAt: null,
          fromField: 'tags',
          fromId,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 2,
          toId: toId3,
          toType: 'tags',
          v,
        },
      ])

      const relationships = await queryActiveRelationships('posts', fromId)

      expect(relationships.length).toBe(3)
      expect(relationships.map((r) => r.toId).sort()).toEqual([toId1, toId2, toId3].sort())
    })
  })

  describe('softDeleteRelationships (FROM document)', () => {
    it('should soft-delete all outgoing relationships from a document', async () => {
      const v = generateVersion()
      const fromId = generateId()
      const toId1 = generateId()
      const toId2 = generateId()

      // Create relationships
      await insertRelationships(adapter.clickhouse!, adapter.table, [
        {
          deletedAt: null,
          fromField: 'author',
          fromId,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: toId1,
          toType: 'users',
          v,
        },
        {
          deletedAt: null,
          fromField: 'category',
          fromId,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: toId2,
          toType: 'categories',
          v,
        },
      ])

      // Verify relationships exist
      let relationships = await queryActiveRelationships('posts', fromId)
      expect(relationships.length).toBe(2)

      // Soft-delete
      const deleteV = generateVersion()
      await softDeleteRelationships(adapter.clickhouse!, adapter.table, {
        fromId,
        fromType: 'posts',
        ns: adapter.namespace,
        v: deleteV,
      })

      // Verify relationships are deleted
      relationships = await queryActiveRelationships('posts', fromId)
      expect(relationships.length).toBe(0)
    })

    it('should not affect relationships from other documents', async () => {
      const v = generateVersion()
      const fromId1 = generateId()
      const fromId2 = generateId()
      const toId = generateId()

      // Create relationships from two different documents
      await insertRelationships(adapter.clickhouse!, adapter.table, [
        {
          deletedAt: null,
          fromField: 'author',
          fromId: fromId1,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId,
          toType: 'users',
          v,
        },
        {
          deletedAt: null,
          fromField: 'author',
          fromId: fromId2,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId,
          toType: 'users',
          v,
        },
      ])

      // Soft-delete only from first document
      const deleteV = generateVersion()
      await softDeleteRelationships(adapter.clickhouse!, adapter.table, {
        fromId: fromId1,
        fromType: 'posts',
        ns: adapter.namespace,
        v: deleteV,
      })

      // Verify only first document's relationships are deleted
      const rels1 = await queryActiveRelationships('posts', fromId1)
      const rels2 = await queryActiveRelationships('posts', fromId2)

      expect(rels1.length).toBe(0)
      expect(rels2.length).toBe(1)
    })
  })

  describe('softDeleteRelationshipsToDocument (TO document)', () => {
    it('should soft-delete all incoming relationships to a document', async () => {
      const v = generateVersion()
      const userId = generateId()
      const postId1 = generateId()
      const postId2 = generateId()

      // Create relationships pointing to the user
      await insertRelationships(adapter.clickhouse!, adapter.table, [
        {
          deletedAt: null,
          fromField: 'author',
          fromId: postId1,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: userId,
          toType: 'users',
          v,
        },
        {
          deletedAt: null,
          fromField: 'author',
          fromId: postId2,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: userId,
          toType: 'users',
          v,
        },
      ])

      // Verify relationships exist
      let relationships = await queryActiveRelationships(undefined, undefined, 'users', userId)
      expect(relationships.length).toBe(2)

      // Soft-delete all relationships TO this user (simulating user deletion)
      const deleteV = generateVersion()
      await softDeleteRelationshipsToDocument(adapter.clickhouse!, adapter.table, {
        ns: adapter.namespace,
        toId: userId,
        toType: 'users',
        v: deleteV,
      })

      // Verify relationships are deleted
      relationships = await queryActiveRelationships(undefined, undefined, 'users', userId)
      expect(relationships.length).toBe(0)
    })

    it('should not affect relationships to other documents', async () => {
      const v = generateVersion()
      const userId1 = generateId()
      const userId2 = generateId()
      const postId = generateId()

      // Create relationships pointing to two different users
      await insertRelationships(adapter.clickhouse!, adapter.table, [
        {
          deletedAt: null,
          fromField: 'author',
          fromId: postId,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: userId1,
          toType: 'users',
          v,
        },
        {
          deletedAt: null,
          fromField: 'reviewer',
          fromId: postId,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: userId2,
          toType: 'users',
          v,
        },
      ])

      // Soft-delete only relationships TO first user
      const deleteV = generateVersion()
      await softDeleteRelationshipsToDocument(adapter.clickhouse!, adapter.table, {
        ns: adapter.namespace,
        toId: userId1,
        toType: 'users',
        v: deleteV,
      })

      // Verify only first user's incoming relationships are deleted
      const rels1 = await queryActiveRelationships(undefined, undefined, 'users', userId1)
      const rels2 = await queryActiveRelationships(undefined, undefined, 'users', userId2)

      expect(rels1.length).toBe(0)
      expect(rels2.length).toBe(1)
    })
  })

  describe('bidirectional cleanup scenario', () => {
    it('should clean up both incoming and outgoing relationships when document is deleted', async () => {
      const v = generateVersion()

      // Create a "post" that has an author (outgoing) and is referenced by comments (incoming)
      const postId = generateId()
      const authorId = generateId()
      const commentId1 = generateId()
      const commentId2 = generateId()

      // Post -> Author (outgoing relationship from post)
      // Comment1 -> Post (incoming relationship to post)
      // Comment2 -> Post (incoming relationship to post)
      await insertRelationships(adapter.clickhouse!, adapter.table, [
        // Post's outgoing relationship
        {
          deletedAt: null,
          fromField: 'author',
          fromId: postId,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: authorId,
          toType: 'users',
          v,
        },
        // Comments' incoming relationships to post
        {
          deletedAt: null,
          fromField: 'post',
          fromId: commentId1,
          fromType: 'comments',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: postId,
          toType: 'posts',
          v,
        },
        {
          deletedAt: null,
          fromField: 'post',
          fromId: commentId2,
          fromType: 'comments',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: postId,
          toType: 'posts',
          v,
        },
      ])

      // Verify all relationships exist
      const outgoing = await queryActiveRelationships('posts', postId)
      const incoming = await queryActiveRelationships(undefined, undefined, 'posts', postId)
      expect(outgoing.length).toBe(1)
      expect(incoming.length).toBe(2)

      // Simulate deleting the post - should clean up both directions
      const deleteV = generateVersion()

      await Promise.all([
        // Clean outgoing (post -> author)
        softDeleteRelationships(adapter.clickhouse!, adapter.table, {
          fromId: postId,
          fromType: 'posts',
          ns: adapter.namespace,
          v: deleteV,
        }),
        // Clean incoming (comments -> post)
        softDeleteRelationshipsToDocument(adapter.clickhouse!, adapter.table, {
          ns: adapter.namespace,
          toId: postId,
          toType: 'posts',
          v: deleteV,
        }),
      ])

      // Verify all relationships are cleaned up
      const outgoingAfter = await queryActiveRelationships('posts', postId)
      const incomingAfter = await queryActiveRelationships(undefined, undefined, 'posts', postId)
      expect(outgoingAfter.length).toBe(0)
      expect(incomingAfter.length).toBe(0)

      // Verify author's other relationships are unaffected
      // (author should still have no incoming from post since we deleted it)
      const authorIncoming = await queryActiveRelationships(undefined, undefined, 'users', authorId)
      expect(authorIncoming.length).toBe(0)
    })
  })
})
