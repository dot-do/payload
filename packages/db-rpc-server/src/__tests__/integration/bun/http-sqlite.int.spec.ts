/**
 * Integration tests for db-rpc with HTTP transport and SQLite backend
 *
 * These tests verify the full client-server round trip using:
 * - HTTP batch transport (capnweb)
 * - SQLite as the underlying database (in-memory)
 */

import type { TestSetup } from '../helpers/setupTestServer.js'

import { sqliteAdapter } from '@payloadcms/db-sqlite'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { setupTestServer } from '../helpers/setupTestServer.js'

// Run SQLite tests if PAYLOAD_DATABASE=sqlite or TEST_SQLITE is set
const skipTests = process.env.PAYLOAD_DATABASE !== 'sqlite' && !process.env.TEST_SQLITE

describe.skipIf(skipTests)('db-rpc HTTP + SQLite Integration', () => {
  let setup: TestSetup

  beforeAll(async () => {
    // Setup server with SQLite (in-memory)
    setup = await setupTestServer(
      sqliteAdapter({
        client: {
          url: ':memory:',
        },
      }),
    )
  }, 60000)

  afterAll(async () => {
    if (setup?.cleanup) {
      await setup.cleanup()
    }
  })

  describe('CRUD Operations', () => {
    it('should create a document via RPC', async () => {
      const doc = await setup.clientPayload.create({
        collection: 'posts',
        data: {
          content: 'SQLite test content',
          title: 'SQLite Test Post',
        },
      })

      expect(doc).toBeDefined()
      expect(doc.id).toBeDefined()
      expect(doc.title).toBe('SQLite Test Post')
    })

    it('should find documents via RPC', async () => {
      await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'SQLite Find Test 1' },
      })
      await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'SQLite Find Test 2' },
      })

      const result = await setup.clientPayload.find({
        collection: 'posts',
        where: {
          title: { contains: 'SQLite Find Test' },
        },
      })

      expect(result.docs.length).toBeGreaterThanOrEqual(2)
    })

    it('should find one document by ID via RPC', async () => {
      const created = await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'SQLite Find One Test' },
      })

      const found = await setup.clientPayload.findByID({
        collection: 'posts',
        id: created.id,
      })

      expect(found).toBeDefined()
      expect(found.title).toBe('SQLite Find One Test')
    })

    it('should update a document via RPC', async () => {
      const created = await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'SQLite Update Test' },
      })

      const updated = await setup.clientPayload.update({
        collection: 'posts',
        data: { title: 'SQLite Updated' },
        id: created.id,
      })

      expect(updated.title).toBe('SQLite Updated')
    })

    it('should delete a document via RPC', async () => {
      const created = await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'SQLite Delete Test' },
      })

      await setup.clientPayload.delete({
        collection: 'posts',
        id: created.id,
      })

      await expect(
        setup.clientPayload.findByID({
          collection: 'posts',
          id: created.id,
        }),
      ).rejects.toThrow()
    })

    it('should count documents via RPC', async () => {
      await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'SQLite Count Test 1' },
      })
      await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'SQLite Count Test 2' },
      })

      const result = await setup.clientPayload.count({
        collection: 'posts',
        where: {
          title: { contains: 'SQLite Count Test' },
        },
      })

      expect(result.totalDocs).toBeGreaterThanOrEqual(2)
    })
  })

  describe('Server Info', () => {
    // TODO: Debug why serverInfo isn't being set on the client adapter
    // The getServerInfo RPC call works but the property isn't being exposed
    it.skip('should have server info for SQLite', async () => {
      const adapter = setup.clientPayload.db as { serverInfo?: { adapterName: string } }
      expect(adapter.serverInfo).toBeDefined()
      expect(adapter.serverInfo?.adapterName).toBe('sqlite')
    })
  })
})
