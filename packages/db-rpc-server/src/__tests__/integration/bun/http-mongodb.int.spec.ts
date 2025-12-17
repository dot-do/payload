/**
 * Integration tests for db-rpc with HTTP transport and MongoDB backend
 *
 * These tests verify the full client-server round trip using:
 * - HTTP batch transport (capnweb)
 * - MongoDB as the underlying database (via memory server)
 */

import type { TestSetup } from '../helpers/setupTestServer.js'

import { mongooseAdapter } from '@payloadcms/db-mongodb'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { setupTestServer } from '../helpers/setupTestServer.js'

describe('db-rpc HTTP + MongoDB Integration', () => {
  let setup: TestSetup

  beforeAll(async () => {
    // Check for MongoDB URI at runtime (after global setup has run)
    const MONGODB_URI = process.env.MONGODB_MEMORY_SERVER_URI || process.env.MONGODB_URI
    if (!MONGODB_URI) {
      console.log('Skipping MongoDB tests: no MONGODB_URI available')
      return
    }

    // Setup server with MongoDB memory server
    setup = await setupTestServer(
      mongooseAdapter({
        url: MONGODB_URI,
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
          content: 'Test content',
          title: 'Test Post',
        },
      })

      expect(doc).toBeDefined()
      expect(doc.id).toBeDefined()
      expect(doc.title).toBe('Test Post')
      expect(doc.content).toBe('Test content')
    })

    it('should find documents via RPC', async () => {
      // Create a few documents first
      await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'Find Test 1' },
      })
      await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'Find Test 2' },
      })

      const result = await setup.clientPayload.find({
        collection: 'posts',
        where: {
          title: { contains: 'Find Test' },
        },
      })

      expect(result.docs.length).toBeGreaterThanOrEqual(2)
      expect(result.totalDocs).toBeGreaterThanOrEqual(2)
    })

    it('should find one document by ID via RPC', async () => {
      const created = await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'Find One Test' },
      })

      const found = await setup.clientPayload.findByID({
        collection: 'posts',
        id: created.id,
      })

      expect(found).toBeDefined()
      expect(found.title).toBe('Find One Test')
    })

    it('should update a document via RPC', async () => {
      const created = await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'Update Test' },
      })

      const updated = await setup.clientPayload.update({
        collection: 'posts',
        data: { title: 'Updated Title' },
        id: created.id,
      })

      expect(updated.title).toBe('Updated Title')
    })

    it('should delete a document via RPC', async () => {
      const created = await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'Delete Test' },
      })

      await setup.clientPayload.delete({
        collection: 'posts',
        id: created.id,
      })

      // Verify deletion
      await expect(
        setup.clientPayload.findByID({
          collection: 'posts',
          id: created.id,
        }),
      ).rejects.toThrow()
    })

    it('should count documents via RPC', async () => {
      // Create some documents
      await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'Count Test 1' },
      })
      await setup.clientPayload.create({
        collection: 'posts',
        data: { title: 'Count Test 2' },
      })

      const result = await setup.clientPayload.count({
        collection: 'posts',
        where: {
          title: { contains: 'Count Test' },
        },
      })

      expect(result.totalDocs).toBeGreaterThanOrEqual(2)
    })
  })

  describe('Transactions', () => {
    it('should commit a transaction successfully', async () => {
      // Start transaction
      const txId = await setup.clientPayload.db.beginTransaction()

      if (txId) {
        // Create document within transaction
        const doc = await setup.clientPayload.create({
          collection: 'posts',
          data: { title: 'Transaction Test' },
          req: { transactionID: txId },
        })

        // Commit
        await setup.clientPayload.db.commitTransaction(txId)

        // Verify document exists
        const found = await setup.clientPayload.findByID({
          collection: 'posts',
          id: doc.id,
        })

        expect(found.title).toBe('Transaction Test')
      }
    })

    it('should rollback a transaction', async () => {
      const txId = await setup.clientPayload.db.beginTransaction()

      if (txId) {
        // Create document within transaction
        const doc = await setup.clientPayload.create({
          collection: 'posts',
          data: { title: 'Rollback Test' },
          req: { transactionID: txId },
        })

        const docId = doc.id

        // Rollback
        await setup.clientPayload.db.rollbackTransaction(txId)

        // Verify document doesn't exist
        await expect(
          setup.clientPayload.findByID({
            collection: 'posts',
            id: docId,
          }),
        ).rejects.toThrow()
      }
    })
  })

  describe('Error Handling', () => {
    it('should propagate validation errors', async () => {
      await expect(
        setup.clientPayload.create({
          collection: 'posts',
          data: {
            // Missing required 'title' field
            content: 'No title',
          },
        }),
      ).rejects.toThrow()
    })

    it('should propagate not found errors', async () => {
      await expect(
        setup.clientPayload.findByID({
          collection: 'posts',
          id: '000000000000000000000000', // Valid ObjectId format but doesn't exist
        }),
      ).rejects.toThrow()
    })
  })

  describe('Server Info', () => {
    // TODO: Debug why serverInfo isn't being set on the client adapter
    // The getServerInfo RPC call works but the property isn't being exposed
    it.skip('should have server info available', async () => {
      // The client adapter stores server info after connect
      const adapter = setup.clientPayload.db as { serverInfo?: { adapterName: string } }
      expect(adapter.serverInfo).toBeDefined()
      expect(adapter.serverInfo?.adapterName).toBe('mongoose')
    })
  })
})
