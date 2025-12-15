import { describe, expect, it } from 'vitest'

import { generateVersion } from '../utilities/generateId.js'

describe('eventual consistency', () => {
  describe('generateVersion', () => {
    it('should generate monotonically increasing versions', () => {
      const versions: number[] = []
      for (let i = 0; i < 100; i++) {
        versions.push(generateVersion())
      }

      // Each version should be greater than the previous
      for (let i = 1; i < versions.length; i++) {
        expect(versions[i]).toBeGreaterThan(versions[i - 1]!)
      }
    })

    it('should generate versions based on current time', () => {
      const before = Date.now()
      const version = generateVersion()
      const after = Date.now()

      expect(version).toBeGreaterThanOrEqual(before)
      expect(version).toBeLessThanOrEqual(after + 100) // Allow small drift
    })

    it('should handle rapid calls within same millisecond', () => {
      // Generate many versions rapidly
      const versions = new Set<number>()
      for (let i = 0; i < 1000; i++) {
        versions.add(generateVersion())
      }

      // All versions should be unique
      expect(versions.size).toBe(1000)
    })
  })
})
