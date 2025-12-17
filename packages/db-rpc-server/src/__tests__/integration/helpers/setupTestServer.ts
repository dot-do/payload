/**
 * Helper for setting up test server and client
 *
 * Uses MongoDB Memory Server (via global setup) or SQLite in-memory
 */

import type { DatabaseAdapterObj, Payload } from 'payload'

import { serve } from '@hono/node-server'
import { rpcAdapter } from '@payloadcms/db-rpc'
import { getPayload } from 'payload'

import { createRpcServer } from '../../../middleware/hono.js'
import { createTestConfig } from '../fixtures/testConfig.js'

export interface TestSetup {
  cleanup: () => Promise<void>
  /** Configured RPC client adapter */
  clientAdapter: DatabaseAdapterObj
  /** Client Payload instance (using RPC adapter) */
  clientPayload: Payload
  /** Server port */
  port: number
  /** Server Payload instance (using real DB adapter) */
  serverPayload: Payload
  /** Server URL */
  serverUrl: string
  /** User token for authentication */
  token: string
}

/**
 * Setup test server with the given database adapter
 */
export async function setupTestServer(dbAdapter: DatabaseAdapterObj): Promise<TestSetup> {
  // Find an available port
  const port = 4000 + Math.floor(Math.random() * 1000)
  const serverUrl = `http://localhost:${port}/rpc`

  // Initialize server Payload with the actual database
  const serverConfig = createTestConfig(dbAdapter)
  const serverPayload = await getPayload({ config: serverConfig })

  // Create a test user and get a token
  const testEmail = `test-${port}@example.com`
  const testPassword = 'test-password-123'

  // Find or create user
  const existingUser = await serverPayload.find({
    collection: 'users',
    where: { email: { equals: testEmail } },
  })

  if (existingUser.totalDocs === 0) {
    // Create the test user
    await serverPayload.create({
      collection: 'users',
      data: {
        email: testEmail,
        password: testPassword,
      },
    })
  }

  // Login to get a token
  const loginResult = await serverPayload.login({
    collection: 'users',
    data: {
      email: testEmail,
      password: testPassword,
    },
  })

  const token = loginResult.token!

  // Create and start the RPC server
  const app = createRpcServer({
    adapter: serverPayload.db,
    payload: serverPayload,
  })

  const server = serve({
    fetch: app.fetch,
    port,
  })

  // Wait a bit for server to start
  await new Promise((resolve) => setTimeout(resolve, 100))

  // Create the client adapter
  const clientAdapter = rpcAdapter({
    token,
    url: serverUrl,
  })

  // Initialize client Payload with RPC adapter
  const clientConfig = createTestConfig(clientAdapter)
  const clientPayload = await getPayload({ config: clientConfig })

  const cleanup = async () => {
    // Clean up test data
    try {
      await serverPayload.delete({
        collection: 'posts',
        where: {},
      })
    } catch {
      // Ignore errors during cleanup
    }

    // Close server
    server.close()

    // Destroy client Payload connection
    if (clientPayload.db.destroy) {
      await clientPayload.db.destroy()
    }

    // Destroy server Payload connection
    if (serverPayload.db.destroy) {
      await serverPayload.db.destroy()
    }
  }

  return {
    cleanup,
    clientAdapter,
    clientPayload,
    port,
    serverPayload,
    serverUrl,
    token,
  }
}
