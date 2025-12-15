import { describe, expect, it } from 'vitest'

import { generateId, generateUlid, generateVersion } from '../utilities/generateId.js'

describe('generateId', () => {
  it('should generate nanoid by default', () => {
    const id = generateId()
    expect(id).toHaveLength(21)
  })

  it('should generate uuid when specified', () => {
    const id = generateId('uuid')
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i)
  })
})

describe('generateUlid', () => {
  it('should generate 26-character ULID', () => {
    const id = generateUlid()
    expect(id).toHaveLength(26)
  })

  it('should be lexicographically sortable by time', async () => {
    const ids: string[] = []
    for (let i = 0; i < 5; i++) {
      ids.push(generateUlid())
      // Small delay to ensure different millisecond timestamps
      await new Promise((resolve) => setTimeout(resolve, 2))
    }
    const sorted = [...ids].sort()
    expect(sorted).toEqual(ids)
  })
})

describe('generateVersion', () => {
  it('should generate monotonically increasing versions', () => {
    const v1 = generateVersion()
    const v2 = generateVersion()
    const v3 = generateVersion()
    expect(v2).toBeGreaterThan(v1)
    expect(v3).toBeGreaterThan(v2)
  })
})
