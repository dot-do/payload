import { describe, expect, it } from 'vitest'

import { combineWhere, QueryBuilder } from '../queries/QueryBuilder.js'

describe('QueryBuilder', () => {
  describe('buildWhereClause', () => {
    it('should handle equals operator', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({ title: { equals: 'test' } })
      expect(where).toContain('title')
      expect(where).toContain('=')
    })

    it('should handle in operator with array', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({
        status: { in: ['draft', 'published'] },
      })
      expect(where).toContain('IN')
    })

    it('should handle empty in array', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({ status: { in: [] } })
      expect(where).toBe('1=0') // Should never match
    })

    it('should handle greater_than operator', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({ count: { greater_than: 10 } })
      expect(where).toContain('>')
    })

    it('should handle nested AND conditions', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({
        and: [{ title: { equals: 'test' } }, { status: { equals: 'published' } }],
      })
      expect(where).toContain('AND')
    })

    it('should handle nested OR conditions', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({
        or: [{ title: { equals: 'a' } }, { title: { equals: 'b' } }],
      })
      expect(where).toContain('OR')
    })

    it('should handle null equals', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({ deletedAt: { equals: null } })
      expect(where).toContain('IS NULL')
    })

    it('should handle exists operator', () => {
      const qb = new QueryBuilder()
      const whereExists = qb.buildWhereClause({ field: { exists: true } })
      expect(whereExists).toContain('IS NOT NULL')

      const qb2 = new QueryBuilder()
      const whereNotExists = qb2.buildWhereClause({ field: { exists: false } })
      expect(whereNotExists).toContain('IS NULL')
    })

    it('should handle contains operator', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({ title: { contains: 'test' } })
      expect(where).toContain('position')
      expect(where).toContain('lower')
    })

    it('should handle like operator', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({ title: { like: 'test' } })
      expect(where).toContain('ILIKE')
    })
  })

  describe('combineWhere', () => {
    it('should combine base and additional where', () => {
      const result = combineWhere('ns = {ns:String}', 'status = {p0:String}')
      expect(result).toBe('ns = {ns:String} AND (status = {p0:String})')
    })

    it('should return base when additional is empty', () => {
      const result = combineWhere('ns = {ns:String}', '')
      expect(result).toBe('ns = {ns:String}')
    })
  })

  describe('getParams', () => {
    it('should collect all parameters', () => {
      const qb = new QueryBuilder()
      qb.buildWhereClause({
        title: { equals: 'test' },
        count: { greater_than: 5 },
      })
      const params = qb.getParams()
      expect(Object.keys(params).length).toBeGreaterThan(0)
    })
  })
})
