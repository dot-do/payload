import type { Where } from 'payload'

/**
 * Query parameters map for ClickHouse parameterized queries
 */
export type QueryParams = Record<string, boolean | null | number | string>

/**
 * Result of building a parameterized query
 */
export interface ParameterizedQuery {
  params: QueryParams
  sql: string
}

/**
 * QueryBuilder creates parameterized SQL queries for ClickHouse
 * Uses {param:Type} syntax for safe parameter binding
 */
export class QueryBuilder {
  private paramCounter = 0
  private params: QueryParams = {}

  /**
   * Apply an operator to a field
   */
  private applyOperator(field: string, operator: string, value: unknown): string {
    const path = this.getFieldPath(field)

    switch (operator) {
      case 'all': {
        if (!Array.isArray(value) || value.length === 0) {
          return '1=1'
        }
        const allConditions = value.map((v) => `has(${path}, ${this.addParam(v)})`).join(' AND ')
        return `(${allConditions})`
      }

      case 'contains':
        if (typeof value !== 'string') {
          return '1=1'
        }
        return `position(lower(toString(${path})), lower(${this.addParam(value)})) > 0`

      case 'equals':
        if (value === null || value === undefined) {
          return `${path} IS NULL`
        }
        return `${path} = ${this.addParam(value)}`

      case 'exists':
        return value === true ? `${path} IS NOT NULL` : `${path} IS NULL`

      case 'greater_than':
        return `${path} > ${this.addParam(value)}`

      case 'greater_than_equal':
        return `${path} >= ${this.addParam(value)}`

      case 'in': {
        if (!Array.isArray(value) || value.length === 0) {
          return '1=0'
        }
        const inValues = value.map((v) => this.addParam(v)).join(', ')
        return `${path} IN (${inValues})`
      }

      case 'less_than':
        return `${path} < ${this.addParam(value)}`

      case 'less_than_equal':
        return `${path} <= ${this.addParam(value)}`

      case 'like':
        if (typeof value !== 'string') {
          return '1=1'
        }
        // Use ILIKE for case-insensitive matching with parameterized value
        return `${path} ILIKE concat('%', ${this.addParam(value)}, '%')`

      case 'near':
        // eslint-disable-next-line no-console
        console.warn('Near operator not fully implemented for ClickHouse')
        return '1=1'

      case 'not_equals':
        if (value === null || value === undefined) {
          return `${path} IS NOT NULL`
        }
        return `${path} != ${this.addParam(value)}`

      case 'not_in': {
        if (!Array.isArray(value) || value.length === 0) {
          return '1=1'
        }
        const notInValues = value.map((v) => this.addParam(v)).join(', ')
        return `${path} NOT IN (${notInValues})`
      }

      default:
        // eslint-disable-next-line no-console
        console.warn(`Unknown operator: ${operator}`)
        return '1=1'
    }
  }

  /**
   * Recursively build conditions from a where object
   */
  private buildConditions(where: Where): string {
    const parts: string[] = []

    for (const [key, value] of Object.entries(where)) {
      if (key === 'and' && Array.isArray(value)) {
        const andConditions = value
          .map((condition) => this.buildConditions(condition))
          .filter(Boolean)
        if (andConditions.length > 0) {
          parts.push(`(${andConditions.join(' AND ')})`)
        }
      } else if (key === 'or' && Array.isArray(value)) {
        const orConditions = value
          .map((condition) => this.buildConditions(condition))
          .filter(Boolean)
        if (orConditions.length > 0) {
          parts.push(`(${orConditions.join(' OR ')})`)
        }
      } else if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        const fieldConditions = this.buildFieldConditions(key, value as Record<string, unknown>)
        if (fieldConditions) {
          parts.push(fieldConditions)
        }
      }
    }

    return parts.join(' AND ')
  }

  /**
   * Build conditions for a single field with operators
   */
  private buildFieldConditions(field: string, operators: Record<string, unknown>): string {
    const conditions: string[] = []
    const knownOperators = [
      'equals',
      'not_equals',
      'like',
      'contains',
      'in',
      'not_in',
      'all',
      'exists',
      'greater_than',
      'greater_than_equal',
      'less_than',
      'less_than_equal',
      'near',
    ]

    for (const [operator, value] of Object.entries(operators)) {
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        const hasOperators = Object.keys(value).some((k) => knownOperators.includes(k))

        if (hasOperators) {
          const nestedConditions = this.buildFieldConditions(
            `${field}.${operator}`,
            value as Record<string, unknown>,
          )
          if (nestedConditions) {
            conditions.push(nestedConditions)
          }
        } else {
          conditions.push(this.applyOperator(field, operator, value))
        }
      } else {
        conditions.push(this.applyOperator(field, operator, value))
      }
    }

    if (conditions.length === 0) {
      return ''
    }

    if (conditions.length === 1) {
      return conditions[0]!
    }

    return `(${conditions.join(' AND ')})`
  }

  /**
   * Get the ClickHouse field path for a Payload field
   */
  private getFieldPath(field: string): string {
    // Sanitize field path - only allow alphanumeric, underscores, dots, and brackets
    const sanitized = field.replace(/[^\w.[\]]/g, '')

    const topLevelFields = [
      'id',
      'ns',
      'type',
      'v',
      'title',
      'createdAt',
      'createdBy',
      'updatedAt',
      'updatedBy',
      'deletedAt',
      'deletedBy',
    ]

    if (topLevelFields.includes(sanitized)) {
      return sanitized
    }

    // Handle version.X paths
    if (sanitized.startsWith('version.')) {
      const versionField = sanitized.slice('version.'.length)
      return `data.${versionField}`
    }

    return `data.${sanitized}`
  }

  /**
   * Add a named parameter (for reuse)
   */
  addNamedParam(name: string, value: unknown): string {
    if (value === null || value === undefined) {
      this.params[name] = null
      return 'NULL'
    }

    if (typeof value === 'string') {
      this.params[name] = value
      return `{${name}:String}`
    }

    if (typeof value === 'number') {
      this.params[name] = value
      return Number.isInteger(value) ? `{${name}:Int64}` : `{${name}:Float64}`
    }

    if (typeof value === 'boolean') {
      this.params[name] = value ? 1 : 0
      return `{${name}:UInt8}`
    }

    this.params[name] = String(value)
    return `{${name}:String}`
  }

  /**
   * Add a parameter and return its placeholder
   */
  addParam(value: unknown, type: 'Float64' | 'Int64' | 'String' | 'UInt8' = 'String'): string {
    const name = `p${this.paramCounter++}`

    if (value === null || value === undefined) {
      this.params[name] = null
      return 'NULL'
    }

    if (typeof value === 'string') {
      this.params[name] = value
      return `{${name}:String}`
    }

    if (typeof value === 'number') {
      if (Number.isNaN(value) || !Number.isFinite(value)) {
        this.params[name] = null
        return 'NULL'
      }
      this.params[name] = value
      return Number.isInteger(value) ? `{${name}:Int64}` : `{${name}:Float64}`
    }

    if (typeof value === 'boolean') {
      this.params[name] = value ? 1 : 0
      return `{${name}:UInt8}`
    }

    if (value instanceof Date) {
      this.params[name] = value.getTime()
      return `fromUnixTimestamp64Milli({${name}:Int64})`
    }

    // For objects/arrays, JSON stringify
    if (typeof value === 'object') {
      this.params[name] = JSON.stringify(value)
      return `{${name}:String}`
    }

    this.params[name] = String(value)
    return `{${name}:String}`
  }

  /**
   * Add a timestamp parameter (milliseconds since epoch)
   */
  addTimestamp(ms: number): string {
    const name = `p${this.paramCounter++}`
    this.params[name] = ms
    return `fromUnixTimestamp64Milli({${name}:Int64})`
  }

  /**
   * Build the base WHERE clause for collection queries
   */
  buildBaseWhere(ns: string, type: string): string {
    const nsParam = this.addNamedParam('ns', ns)
    const typeParam = this.addNamedParam('type', type)
    return `ns = ${nsParam} AND type = ${typeParam} AND deletedAt IS NULL`
  }

  /**
   * Build WHERE clause from Payload's where condition
   */
  buildWhereClause(where: undefined | Where): string {
    if (!where || Object.keys(where).length === 0) {
      return ''
    }
    return this.buildConditions(where)
  }

  /**
   * Get all collected parameters
   */
  getParams(): QueryParams {
    return { ...this.params }
  }
}

/**
 * Combine base WHERE with additional conditions
 */
export function combineWhere(baseWhere: string, additionalWhere: string): string {
  if (!additionalWhere) {
    return baseWhere
  }
  return `${baseWhere} AND (${additionalWhere})`
}
