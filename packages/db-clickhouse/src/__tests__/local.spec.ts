import { describe, expect, it, vi } from 'vitest'

import type { ChdbSession } from '../types.js'

import { clickhouseAdapter } from '../index.js'
import { ChdbClient } from '../local/chdbClient.js'
import { assertValidNamespace, validateNamespace } from '../utilities/sanitize.js'

// Mock the Session class
const mockQuery = vi.fn()
const mockCleanup = vi.fn()

const createMockSession = (): ChdbSession => ({
  cleanup: mockCleanup,
  query: mockQuery,
})

const mockSession = createMockSession()

describe('ChdbClient', () => {
  describe('parameter substitution', () => {
    it('should substitute string parameters', async () => {
      mockQuery.mockReturnValue('')
      const client = new ChdbClient(mockSession as never)

      await client.command({
        query: 'SELECT * FROM data WHERE ns = {ns:String}',
        query_params: { ns: 'test-namespace' },
      })

      expect(mockQuery).toHaveBeenCalledWith("SELECT * FROM data WHERE ns = 'test-namespace'")
    })

    it('should substitute number parameters', async () => {
      mockQuery.mockReturnValue('')
      const client = new ChdbClient(mockSession as never)

      await client.command({
        query: 'SELECT * FROM data WHERE id = {id:Int64}',
        query_params: { id: 12345 },
      })

      expect(mockQuery).toHaveBeenCalledWith('SELECT * FROM data WHERE id = 12345')
    })

    it('should handle NULL values', async () => {
      mockQuery.mockReturnValue('')
      const client = new ChdbClient(mockSession as never)

      await client.command({
        query: 'SELECT * FROM data WHERE deletedAt = {deletedAt:Nullable(DateTime64(3))}',
        query_params: { deletedAt: null },
      })

      expect(mockQuery).toHaveBeenCalledWith('SELECT * FROM data WHERE deletedAt = NULL')
    })

    it('should escape single quotes in strings', async () => {
      mockQuery.mockReturnValue('')
      const client = new ChdbClient(mockSession as never)

      await client.command({
        query: 'SELECT * FROM data WHERE title = {title:String}',
        query_params: { title: "Test's Document" },
      })

      expect(mockQuery).toHaveBeenCalledWith("SELECT * FROM data WHERE title = 'Test''s Document'")
    })

    it('should handle array parameters', async () => {
      mockQuery.mockReturnValue('')
      const client = new ChdbClient(mockSession as never)

      await client.command({
        query: 'SELECT * FROM data WHERE id IN {ids:Array(String)}',
        query_params: { ids: ['id1', 'id2', 'id3'] },
      })

      expect(mockQuery).toHaveBeenCalledWith("SELECT * FROM data WHERE id IN ['id1', 'id2', 'id3']")
    })

    it('should handle boolean parameters', async () => {
      mockQuery.mockReturnValue('')
      const client = new ChdbClient(mockSession as never)

      await client.command({
        query: 'SELECT * FROM data WHERE active = {active:Bool}',
        query_params: { active: true },
      })

      expect(mockQuery).toHaveBeenCalledWith('SELECT * FROM data WHERE active = 1')
    })

    it('should substitute multiple parameters', async () => {
      mockQuery.mockReturnValue('')
      const client = new ChdbClient(mockSession as never)

      await client.command({
        query: 'SELECT * FROM data WHERE ns = {ns:String} AND type = {type:String}',
        query_params: { ns: 'test', type: 'posts' },
      })

      expect(mockQuery).toHaveBeenCalledWith(
        "SELECT * FROM data WHERE ns = 'test' AND type = 'posts'",
      )
    })
  })

  describe('query', () => {
    it('should parse JSONEachRow format', async () => {
      mockQuery.mockReturnValue('{"id":"1","name":"Test"}\n{"id":"2","name":"Another"}')
      const client = new ChdbClient(mockSession as never)

      const result = await client.query({
        format: 'JSONEachRow',
        query: 'SELECT * FROM data',
      })

      const rows = await result.json()
      expect(rows).toEqual([
        { id: '1', name: 'Test' },
        { id: '2', name: 'Another' },
      ])
    })

    it('should handle empty results', async () => {
      mockQuery.mockReturnValue('')
      const client = new ChdbClient(mockSession as never)

      const result = await client.query({
        format: 'JSONEachRow',
        query: 'SELECT * FROM data WHERE 1=0',
      })

      const rows = await result.json()
      expect(rows).toEqual([])
    })

    it('should return raw text', async () => {
      mockQuery.mockReturnValue('some,csv,data')
      const client = new ChdbClient(mockSession as never)

      const result = await client.query({
        format: 'CSV',
        query: 'SELECT * FROM data',
      })

      const text = await result.text()
      expect(text).toBe('some,csv,data')
    })
  })

  describe('close', () => {
    it('should call cleanup on session', async () => {
      const client = new ChdbClient(mockSession as never)
      await client.close()
      expect(mockCleanup).toHaveBeenCalled()
    })
  })
})

describe('namespace validation', () => {
  describe('validateNamespace', () => {
    it('should allow simple alphanumeric namespaces', () => {
      expect(validateNamespace('payload')).toBe(true)
      expect(validateNamespace('myapp')).toBe(true)
      expect(validateNamespace('test123')).toBe(true)
    })

    it('should allow namespaces with underscores and hyphens', () => {
      expect(validateNamespace('my_app')).toBe(true)
      expect(validateNamespace('my-app')).toBe(true)
      expect(validateNamespace('my_app-test')).toBe(true)
    })

    it('should allow namespaces with dots (domain names)', () => {
      expect(validateNamespace('example.com')).toBe(true)
      expect(validateNamespace('api.example.com')).toBe(true)
      expect(validateNamespace('my-app.example.com')).toBe(true)
      expect(validateNamespace('test.localhost')).toBe(true)
    })

    it('should allow namespaces starting with numbers', () => {
      expect(validateNamespace('123test')).toBe(true)
      expect(validateNamespace('1example.com')).toBe(true)
    })

    it('should allow namespaces starting with underscore', () => {
      expect(validateNamespace('_private')).toBe(true)
      expect(validateNamespace('_test.example.com')).toBe(true)
    })

    it('should reject empty namespaces', () => {
      expect(validateNamespace('')).toBe(false)
    })

    it('should reject namespaces with invalid characters', () => {
      expect(validateNamespace('my app')).toBe(false) // space
      expect(validateNamespace('my@app')).toBe(false) // @
      expect(validateNamespace('my/app')).toBe(false) // /
      expect(validateNamespace("my'app")).toBe(false) // quote
    })

    it('should reject namespaces starting with dot or hyphen', () => {
      expect(validateNamespace('.example')).toBe(false)
      expect(validateNamespace('-example')).toBe(false)
    })
  })

  describe('assertValidNamespace', () => {
    it('should return valid namespace', () => {
      expect(assertValidNamespace('example.com')).toBe('example.com')
      expect(assertValidNamespace('my-app')).toBe('my-app')
    })

    it('should throw for invalid namespace', () => {
      expect(() => assertValidNamespace('')).toThrow('Invalid namespace')
      expect(() => assertValidNamespace('my app')).toThrow('Invalid namespace')
      expect(() => assertValidNamespace('.example')).toThrow('Invalid namespace')
    })
  })
})

describe('clickhouseAdapter with session option', () => {
  it('should accept a session directly', () => {
    const session = createMockSession()

    // Should not throw - session is a valid alternative to url
    const adapter = clickhouseAdapter({ session })

    expect(adapter.name).toBe('clickhouse')
    expect(adapter.defaultIDType).toBe('text')
  })

  it('should throw if no client, session, or url provided', () => {
    expect(() => clickhouseAdapter({})).toThrow(
      'ClickHouse adapter requires either a client, session, or url to be provided',
    )
  })

  it('should accept a pre-configured client', () => {
    const mockClient = {
      close: vi.fn(),
      command: vi.fn(),
      query: vi.fn(),
    }

    const adapter = clickhouseAdapter({ client: mockClient })

    expect(adapter.name).toBe('clickhouse')
  })

  it('should accept a url', () => {
    const adapter = clickhouseAdapter({ url: 'http://localhost:8123' })

    expect(adapter.name).toBe('clickhouse')
  })

  it('should pass session to config', () => {
    const session = createMockSession()
    const adapter = clickhouseAdapter({ session })

    // The adapter returns an init function that creates the actual adapter
    // We verify the adapter was created successfully with the session
    expect(adapter.init).toBeDefined()
    expect(typeof adapter.init).toBe('function')
  })
})
