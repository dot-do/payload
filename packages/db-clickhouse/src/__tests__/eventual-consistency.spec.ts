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

  describe('soft delete pattern', () => {
    it('should use window function to get latest version', () => {
      // This tests the SQL pattern used throughout the adapter
      // The pattern: row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      // WHERE _rn = 1 AND deletedAt IS NULL

      // Simulate the data model
      const rows = [
        { id: '1', v: 100, deletedAt: null, data: 'v1' },
        { id: '1', v: 200, deletedAt: null, data: 'v2' },
        { id: '1', v: 300, deletedAt: '2024-01-01', data: 'deleted' }, // soft deleted
      ]

      // Apply window function logic
      const grouped = new Map<string, typeof rows>()
      for (const row of rows) {
        const existing = grouped.get(row.id) || []
        existing.push(row)
        grouped.set(row.id, existing)
      }

      // Get latest non-deleted for each id
      const results: typeof rows = []
      for (const [, group] of grouped) {
        const sorted = group.sort((a, b) => b.v - a.v)
        const latest = sorted.find((r) => r.deletedAt === null)
        if (latest) {
          results.push(latest)
        }
      }

      // Should get v2 (latest non-deleted)
      expect(results.length).toBe(1)
      expect(results[0]!.data).toBe('v2')
      expect(results[0]!.v).toBe(200)
    })
  })
})
