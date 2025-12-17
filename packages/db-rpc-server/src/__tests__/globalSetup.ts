/**
 * Global setup for integration tests
 *
 * Starts MongoDB Memory Server before tests run
 */

import { MongoMemoryReplSet } from 'mongodb-memory-server'

declare global {
  // eslint-disable-next-line no-var
  var _mongoMemoryServer: MongoMemoryReplSet | undefined
}

// eslint-disable-next-line no-restricted-exports
export default async () => {
  // Skip if MONGODB_URI is already provided
  if (process.env.MONGODB_URI || process.env.MONGODB_MEMORY_SERVER_URI) {
    return
  }

  console.log('Starting MongoDB Memory Server...')
  // Use unique database name to avoid collisions between test runs
  const dbName = `payloadrpctest_${Date.now()}`
  const db = await MongoMemoryReplSet.create({
    replSet: {
      count: 1, // Use single replica to speed up tests
      dbName,
    },
  })

  await db.waitUntilRunning()

  global._mongoMemoryServer = db
  // Get the URI and insert database name before any query params
  const baseUri = db.getUri()
  // getUri() returns something like: mongodb://127.0.0.1:27017/?replicaSet=testset
  // We need to insert the database name before the ?
  const uri = baseUri.includes('?')
    ? baseUri.replace('?', `${dbName}?`) + '&retryWrites=true'
    : `${baseUri}${dbName}?retryWrites=true`
  process.env.MONGODB_MEMORY_SERVER_URI = uri
  console.log(`MongoDB Memory Server started (uri: ${uri})`)
}
