/**
 * Global teardown for integration tests
 *
 * Stops MongoDB Memory Server after tests complete
 */

import type { MongoMemoryReplSet } from 'mongodb-memory-server'

declare global {
  // eslint-disable-next-line no-var
  var _mongoMemoryServer: MongoMemoryReplSet | undefined
}

// eslint-disable-next-line no-restricted-exports
export default async () => {
  if (global._mongoMemoryServer) {
    console.log('Stopping MongoDB Memory Server...')
    await global._mongoMemoryServer.stop()
    console.log('MongoDB Memory Server stopped')
  }
}
