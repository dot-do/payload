import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import type { ClickHouseAdapter } from '../types.js'

import { generateId, generateVersion } from '../utilities/generateId.js'
import { insertRelationships } from '../utilities/relationships.js'
import { resolveJoins } from '../utilities/resolveJoins.js'
import { setupTestAdapter } from './helpers/setupTestAdapter.js'

describe.skipIf(!process.env.CLICKHOUSE_TEST)('resolveJoins Integration Tests', () => {
  let adapter: ClickHouseAdapter
  let cleanup: () => Promise<void>

  beforeAll(async () => {
    const setup = await setupTestAdapter()
    adapter = setup.adapter
    cleanup = setup.cleanup

    // Mock minimal Payload structure for resolveJoins
    adapter.payload = {
      collections: {
        posts: {
          config: {
            joins: {
              comments: [
                {
                  field: {
                    collection: 'comments',
                    defaultLimit: 10,
                    name: 'comments',
                    on: 'post',
                  },
                  joinPath: 'comments',
                },
              ],
            },
            polymorphicJoins: [],
            slug: 'posts',
          },
        },
        users: {
          config: {
            joins: {
              posts: [
                {
                  field: {
                    collection: 'posts',
                    defaultLimit: 10,
                    name: 'authoredPosts',
                    on: 'author',
                  },
                  joinPath: 'authoredPosts',
                },
              ],
            },
            polymorphicJoins: [],
            slug: 'users',
          },
        },
      },
    } as any
  })

  afterAll(async () => {
    await cleanup()
  })

  describe('basic join resolution', () => {
    it('should resolve simple joins', async () => {
      const v = generateVersion()
      const postId = generateId()
      const commentId1 = generateId()
      const commentId2 = generateId()

      // Create relationships: comments -> post
      await insertRelationships(adapter.clickhouse!, adapter.table, [
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

      // Create a mock document
      const docs: Record<string, unknown>[] = [{ id: postId, title: 'Test Post' }]

      // Resolve joins
      await resolveJoins({
        adapter,
        collectionSlug: 'posts',
        docs,
        joins: { comments: { limit: 10 } },
      })

      // Verify joins were resolved
      expect(docs[0]!.comments).toBeDefined()
      const commentsJoin = docs[0]!.comments as { docs: string[]; hasNextPage: boolean }
      expect(commentsJoin.docs).toHaveLength(2)
      expect(commentsJoin.docs).toContain(commentId1)
      expect(commentsJoin.docs).toContain(commentId2)
      expect(commentsJoin.hasNextPage).toBe(false)
    })

    it('should return empty array when no relationships exist', async () => {
      const postId = generateId()

      const docs: Record<string, unknown>[] = [{ id: postId, title: 'Lonely Post' }]

      await resolveJoins({
        adapter,
        collectionSlug: 'posts',
        docs,
        joins: { comments: { limit: 10 } },
      })

      const commentsJoin = docs[0]!.comments as { docs: string[]; hasNextPage: boolean }
      expect(commentsJoin.docs).toHaveLength(0)
      expect(commentsJoin.hasNextPage).toBe(false)
    })

    it('should not modify docs when joins is undefined', async () => {
      const postId = generateId()
      const docs: Record<string, unknown>[] = [{ id: postId, title: 'Test Post' }]

      await resolveJoins({
        adapter,
        collectionSlug: 'posts',
        docs,
        joins: undefined,
      })

      expect(docs[0]!.comments).toBeUndefined()
    })

    it('should not modify docs when docs array is empty', async () => {
      const docs: Record<string, unknown>[] = []

      await resolveJoins({
        adapter,
        collectionSlug: 'posts',
        docs,
        joins: { comments: { limit: 10 } },
      })

      expect(docs).toHaveLength(0)
    })
  })

  describe('pagination', () => {
    it('should paginate join results with limit', async () => {
      const v = generateVersion()
      const postId = generateId()

      // Create 5 comments
      const commentIds = Array.from({ length: 5 }, () => generateId())
      await insertRelationships(
        adapter.clickhouse!,
        adapter.table,
        commentIds.map((commentId) => ({
          deletedAt: null,
          fromField: 'post',
          fromId: commentId,
          fromType: 'comments',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: postId,
          toType: 'posts',
          v,
        })),
      )

      const docs: Record<string, unknown>[] = [{ id: postId }]

      // Request only 2
      await resolveJoins({
        adapter,
        collectionSlug: 'posts',
        docs,
        joins: { comments: { limit: 2 } },
      })

      const commentsJoin = docs[0]!.comments as { docs: string[]; hasNextPage: boolean }
      expect(commentsJoin.docs).toHaveLength(2)
      expect(commentsJoin.hasNextPage).toBe(true)
    })

    it('should support pagination with page parameter', async () => {
      const v = generateVersion()
      const postId = generateId()

      // Create 5 comments
      const commentIds = Array.from({ length: 5 }, () => generateId())
      await insertRelationships(
        adapter.clickhouse!,
        adapter.table,
        commentIds.map((commentId) => ({
          deletedAt: null,
          fromField: 'post',
          fromId: commentId,
          fromType: 'comments',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: postId,
          toType: 'posts',
          v,
        })),
      )

      const docs: Record<string, unknown>[] = [{ id: postId }]

      // Request page 2 with limit 2
      await resolveJoins({
        adapter,
        collectionSlug: 'posts',
        docs,
        joins: { comments: { limit: 2, page: 2 } },
      })

      const commentsJoin = docs[0]!.comments as { docs: string[]; hasNextPage: boolean }
      expect(commentsJoin.docs).toHaveLength(2)
      expect(commentsJoin.hasNextPage).toBe(true) // Still has page 3 with 1 item
    })

    it('should include totalDocs when count is requested', async () => {
      const v = generateVersion()
      const postId = generateId()

      // Create 5 comments
      const commentIds = Array.from({ length: 5 }, () => generateId())
      await insertRelationships(
        adapter.clickhouse!,
        adapter.table,
        commentIds.map((commentId) => ({
          deletedAt: null,
          fromField: 'post',
          fromId: commentId,
          fromType: 'comments',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: postId,
          toType: 'posts',
          v,
        })),
      )

      const docs: Record<string, unknown>[] = [{ id: postId }]

      await resolveJoins({
        adapter,
        collectionSlug: 'posts',
        docs,
        joins: { comments: { count: true, limit: 2 } },
      })

      const commentsJoin = docs[0]!.comments as {
        docs: string[]
        hasNextPage: boolean
        totalDocs: number
      }
      expect(commentsJoin.totalDocs).toBe(5)
    })

    it('should return all results when limit is 0', async () => {
      const v = generateVersion()
      const postId = generateId()

      // Create 5 comments
      const commentIds = Array.from({ length: 5 }, () => generateId())
      await insertRelationships(
        adapter.clickhouse!,
        adapter.table,
        commentIds.map((commentId) => ({
          deletedAt: null,
          fromField: 'post',
          fromId: commentId,
          fromType: 'comments',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: postId,
          toType: 'posts',
          v,
        })),
      )

      const docs: Record<string, unknown>[] = [{ id: postId }]

      await resolveJoins({
        adapter,
        collectionSlug: 'posts',
        docs,
        joins: { comments: { limit: 0 } },
      })

      const commentsJoin = docs[0]!.comments as { docs: string[]; hasNextPage: boolean }
      expect(commentsJoin.docs).toHaveLength(5)
      expect(commentsJoin.hasNextPage).toBe(false)
    })
  })

  describe('multiple documents', () => {
    it('should resolve joins for multiple documents', async () => {
      const v = generateVersion()
      const postId1 = generateId()
      const postId2 = generateId()
      const commentId1 = generateId()
      const commentId2 = generateId()
      const commentId3 = generateId()

      // Create relationships
      await insertRelationships(adapter.clickhouse!, adapter.table, [
        // Post 1 has 2 comments
        {
          deletedAt: null,
          fromField: 'post',
          fromId: commentId1,
          fromType: 'comments',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: postId1,
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
          toId: postId1,
          toType: 'posts',
          v,
        },
        // Post 2 has 1 comment
        {
          deletedAt: null,
          fromField: 'post',
          fromId: commentId3,
          fromType: 'comments',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: postId2,
          toType: 'posts',
          v,
        },
      ])

      const docs: Record<string, unknown>[] = [{ id: postId1 }, { id: postId2 }]

      await resolveJoins({
        adapter,
        collectionSlug: 'posts',
        docs,
        joins: { comments: { limit: 10 } },
      })

      const post1Comments = docs[0]!.comments as { docs: string[] }
      const post2Comments = docs[1]!.comments as { docs: string[] }

      expect(post1Comments.docs).toHaveLength(2)
      expect(post2Comments.docs).toHaveLength(1)
    })
  })

  describe('soft-deleted relationships', () => {
    it('should not include soft-deleted relationships in results', async () => {
      const v = generateVersion()
      const postId = generateId()
      const commentId1 = generateId()
      const commentId2 = generateId()

      // Create relationships
      await insertRelationships(adapter.clickhouse!, adapter.table, [
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
        // This one is soft-deleted
        {
          deletedAt: v + 1,
          fromField: 'post',
          fromId: commentId2,
          fromType: 'comments',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: postId,
          toType: 'posts',
          v: v + 1,
        },
      ])

      const docs: Record<string, unknown>[] = [{ id: postId }]

      await resolveJoins({
        adapter,
        collectionSlug: 'posts',
        docs,
        joins: { comments: { limit: 10 } },
      })

      const commentsJoin = docs[0]!.comments as { docs: string[] }
      expect(commentsJoin.docs).toHaveLength(1)
      expect(commentsJoin.docs[0]).toBe(commentId1)
    })
  })

  describe('nested join paths', () => {
    it('should resolve joins at nested paths like group.relatedPosts', async () => {
      // Update mock to have nested join path
      adapter.payload = {
        ...adapter.payload,
        collections: {
          ...adapter.payload.collections,
          categories: {
            config: {
              joins: {
                posts: [
                  {
                    field: {
                      collection: 'posts',
                      defaultLimit: 10,
                      name: 'relatedPosts',
                      on: 'category',
                    },
                    joinPath: 'group.relatedPosts',
                    parentIsLocalized: false,
                  },
                ],
              },
              polymorphicJoins: [],
              slug: 'categories',
            },
          },
        },
      } as any

      const v = generateVersion()
      const categoryId = generateId()
      const postId1 = generateId()
      const postId2 = generateId()

      // Create relationships: posts -> category (via 'category' field)
      await insertRelationships(adapter.clickhouse!, adapter.table, [
        {
          deletedAt: null,
          fromField: 'category',
          fromId: postId1,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: categoryId,
          toType: 'categories',
          v,
        },
        {
          deletedAt: null,
          fromField: 'category',
          fromId: postId2,
          fromType: 'posts',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: categoryId,
          toType: 'categories',
          v,
        },
      ])

      // Create a mock document with existing group structure
      const docs: Record<string, unknown>[] = [{ group: { name: 'Test Group' }, id: categoryId }]

      // Resolve joins
      await resolveJoins({
        adapter,
        collectionSlug: 'categories',
        docs,
        joins: { 'group.relatedPosts': { limit: 10 } },
      })

      // Verify joins were resolved at the nested path
      const group = docs[0]!.group as Record<string, unknown>
      expect(group.name).toBe('Test Group') // Original data preserved
      expect(group.relatedPosts).toBeDefined()
      const relatedPostsJoin = group.relatedPosts as { docs: string[]; hasNextPage: boolean }
      expect(relatedPostsJoin.docs).toHaveLength(2)
      expect(relatedPostsJoin.docs).toContain(postId1)
      expect(relatedPostsJoin.docs).toContain(postId2)
      expect(relatedPostsJoin.hasNextPage).toBe(false)
    })

    it('should create intermediate objects for deeply nested paths', async () => {
      // Update mock to have deeply nested join path
      adapter.payload = {
        ...adapter.payload,
        collections: {
          ...adapter.payload.collections,
          categories: {
            config: {
              joins: {
                posts: [
                  {
                    field: {
                      collection: 'posts',
                      defaultLimit: 10,
                      name: 'items',
                      on: 'category',
                    },
                    joinPath: 'deeply.nested.items',
                    parentIsLocalized: false,
                  },
                ],
              },
              polymorphicJoins: [],
              slug: 'categories',
            },
          },
        },
      } as any

      const categoryId = generateId()

      // Create a mock document without the nested structure
      const docs: Record<string, unknown>[] = [{ id: categoryId }]

      // Resolve joins - should create deeply.nested.items even though deeply doesn't exist
      await resolveJoins({
        adapter,
        collectionSlug: 'categories',
        docs,
        joins: { 'deeply.nested.items': { limit: 10 } },
      })

      // Verify intermediate objects were created
      const deeply = docs[0]!.deeply as Record<string, unknown>
      expect(deeply).toBeDefined()
      const nested = deeply.nested as Record<string, unknown>
      expect(nested).toBeDefined()
      const itemsJoin = nested.items as { docs: string[]; hasNextPage: boolean }
      expect(itemsJoin.docs).toHaveLength(0)
      expect(itemsJoin.hasNextPage).toBe(false)
    })
  })

  describe('versions support', () => {
    it('should use parent field when versions=true', async () => {
      const v = generateVersion()
      const parentId = generateId()
      const versionId = generateId()
      const commentId = generateId()

      // Create relationship pointing to parent
      await insertRelationships(adapter.clickhouse!, adapter.table, [
        {
          deletedAt: null,
          fromField: 'post',
          fromId: commentId,
          fromType: 'comments',
          locale: null,
          ns: adapter.namespace,
          position: 0,
          toId: parentId,
          toType: 'posts',
          v,
        },
      ])

      // Document is a version with parent reference
      const docs: Record<string, unknown>[] = [{ id: versionId, parent: parentId }]

      await resolveJoins({
        adapter,
        collectionSlug: 'posts',
        docs,
        joins: { comments: { limit: 10 } },
        versions: true,
      })

      // For version documents, joins are placed under doc.version.<joinPath>
      const version = docs[0]!.version as Record<string, unknown>
      const commentsJoin = version.comments as { docs: string[] }
      expect(commentsJoin.docs).toHaveLength(1)
      expect(commentsJoin.docs[0]).toBe(commentId)
    })
  })
})
