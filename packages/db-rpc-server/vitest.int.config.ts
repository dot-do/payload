import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // Run test files sequentially to avoid database collisions
    fileParallelism: false,
    // Force exit after tests complete (for cleanup of MongoDB memory server)
    forceExit: true,
    globals: true,
    globalSetup: ['./src/__tests__/globalSetup.ts'],
    globalTeardown: ['./src/__tests__/globalTeardown.ts'],
    include: ['src/**/*.int.spec.ts'],
    testTimeout: 60000,
    // Reduce teardown timeout
    teardownTimeout: 5000,
  },
})
