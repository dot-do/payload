import { describe, expect, it } from 'vitest'

import {
  convertID,
  deepMerge,
  extractTitle,
  hasCustomNumericID,
  parseDateTime64ToMs,
  stripSensitiveFields,
  toISOString,
} from '../utilities/transform.js'

describe('transform utilities', () => {
  describe('stripSensitiveFields', () => {
    it('should strip password field', () => {
      const data = { email: 'test@example.com', password: 'secret123', name: 'Test' }
      const result = stripSensitiveFields(data)
      expect(result).toEqual({ email: 'test@example.com', name: 'Test' })
      expect(result).not.toHaveProperty('password')
    })

    it('should strip confirm-password field', () => {
      const data = { email: 'test@example.com', 'confirm-password': 'secret123', name: 'Test' }
      const result = stripSensitiveFields(data)
      expect(result).toEqual({ email: 'test@example.com', name: 'Test' })
      expect(result).not.toHaveProperty('confirm-password')
    })

    it('should strip both password and confirm-password', () => {
      const data = {
        'confirm-password': 'secret123',
        email: 'test@example.com',
        password: 'secret123',
      }
      const result = stripSensitiveFields(data)
      expect(result).toEqual({ email: 'test@example.com' })
    })

    it('should not modify original object', () => {
      const data = { password: 'secret', name: 'Test' }
      stripSensitiveFields(data)
      expect(data).toHaveProperty('password')
    })
  })

  describe('extractTitle', () => {
    it('should extract title from specified field', () => {
      const data = { name: 'My Title', description: 'Desc' }
      const title = extractTitle(data, 'name', 'fallback-id')
      expect(title).toBe('My Title')
    })

    it('should fallback to id when field is not present', () => {
      const data = { description: 'Desc' }
      const title = extractTitle(data, 'name', 'fallback-id')
      expect(title).toBe('fallback-id')
    })

    it('should return empty string when field is empty string', () => {
      // The implementation returns the actual field value if it's a string (even empty)
      const data = { name: '', description: 'Desc' }
      const title = extractTitle(data, 'name', 'fallback-id')
      expect(title).toBe('')
    })

    it('should use common fallback fields when titleField is undefined', () => {
      // When titleField is undefined, extractTitle tries common fields: title, name, label, email, slug
      const data = { name: 'My Title' }
      const title = extractTitle(data, undefined, 'fallback-id')
      expect(title).toBe('My Title') // Uses 'name' as fallback field
    })

    it('should fallback to id when no common fields exist', () => {
      const data = { someOtherField: 'value' }
      const title = extractTitle(data, undefined, 'fallback-id')
      expect(title).toBe('fallback-id')
    })

    it('should convert non-string title to string', () => {
      const data = { title: 123 }
      const title = extractTitle(data, 'title', 'fallback-id')
      expect(title).toBe('123')
    })
  })

  describe('parseDateTime64ToMs', () => {
    it('should parse DateTime64 string format', () => {
      const ms = parseDateTime64ToMs('2024-01-15 10:30:00.500')
      expect(typeof ms).toBe('number')
      expect(ms).toBeGreaterThan(0)
    })

    it('should parse ISO string format', () => {
      const ms = parseDateTime64ToMs('2024-01-15T10:30:00.500Z')
      expect(typeof ms).toBe('number')
      expect(ms).toBeGreaterThan(0)
    })

    it('should parse already ISO format string', () => {
      // Function expects string input, not numeric
      const timestamp = '2024-01-15T10:30:00.500Z'
      const ms = parseDateTime64ToMs(timestamp)
      expect(typeof ms).toBe('number')
      expect(ms).toBeGreaterThan(0)
    })
  })

  describe('toISOString', () => {
    it('should convert DateTime64 string to ISO format', () => {
      const iso = toISOString('2024-01-15 10:30:00.500')
      expect(iso).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$/)
    })

    it('should preserve ISO string format', () => {
      const original = '2024-01-15T10:30:00.500Z'
      const iso = toISOString(original)
      expect(iso).toBe(original)
    })

    it('should return undefined for null/undefined', () => {
      expect(toISOString(null as any)).toBeUndefined()
      expect(toISOString(undefined as any)).toBeUndefined()
    })
  })

  describe('hasCustomNumericID', () => {
    it('should return true for number ID field', () => {
      const fields: Field[] = [{ name: 'id', type: 'number' }]
      expect(hasCustomNumericID(fields)).toBe(true)
    })

    it('should return false for text ID field', () => {
      const fields: Field[] = [{ name: 'id', type: 'text' }]
      expect(hasCustomNumericID(fields)).toBe(false)
    })

    it('should return false when no ID field', () => {
      const fields: Field[] = [{ name: 'name', type: 'text' }]
      expect(hasCustomNumericID(fields)).toBe(false)
    })

    it('should return false for empty fields', () => {
      expect(hasCustomNumericID([])).toBe(false)
    })
  })

  describe('convertID', () => {
    it('should convert string to number when numericID is true', () => {
      expect(convertID('123', true)).toBe(123)
    })

    it('should return string as-is when numericID is false', () => {
      expect(convertID('abc123', false)).toBe('abc123')
    })

    it('should handle numeric string conversion', () => {
      expect(convertID('456', true)).toBe(456)
    })
  })

  describe('deepMerge', () => {
    it('should merge simple objects', () => {
      const target = { a: 1, b: 2 }
      const source = { b: 3, c: 4 }
      const result = deepMerge(target, source)
      expect(result).toEqual({ a: 1, b: 3, c: 4 })
    })

    it('should deep merge nested objects', () => {
      const target = { nested: { a: 1, b: 2 } }
      const source = { nested: { b: 3, c: 4 } }
      const result = deepMerge(target, source)
      expect(result).toEqual({ nested: { a: 1, b: 3, c: 4 } })
    })

    it('should handle arrays (replace, not merge)', () => {
      const target = { arr: [1, 2, 3] }
      const source = { arr: [4, 5] }
      const result = deepMerge(target, source)
      expect(result).toEqual({ arr: [4, 5] })
    })

    it('should handle $inc operator', () => {
      const target = { count: 5 }
      const source = { count: { $inc: 3 } }
      const result = deepMerge(target, source)
      expect(result).toEqual({ count: 8 })
    })

    it('should handle $remove operator for arrays', () => {
      const target = {
        items: [
          { relationTo: 'posts', value: 'id1' },
          { relationTo: 'posts', value: 'id2' },
        ],
      }
      const source = { items: { $remove: { relationTo: 'posts', value: 'id1' } } }
      const result = deepMerge(target, source)
      expect(result.items).toEqual([{ relationTo: 'posts', value: 'id2' }])
    })

    it('should not modify original objects', () => {
      const target = { a: 1 }
      const source = { b: 2 }
      deepMerge(target, source)
      expect(target).toEqual({ a: 1 })
      expect(source).toEqual({ b: 2 })
    })

    it('should handle null values', () => {
      const target = { a: 1, b: 2 }
      const source = { b: null }
      const result = deepMerge(target, source)
      expect(result).toEqual({ a: 1, b: null })
    })
  })
})
