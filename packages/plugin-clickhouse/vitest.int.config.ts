import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    include: ['src/__tests__/**/*.int.spec.ts'],
    testTimeout: 30000,
  },
})
