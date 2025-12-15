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
 * GeoJSON coordinate type - [longitude, latitude]
 */
type GeoJSONCoordinate = [number, number]

/**
 * GeoJSON Polygon type
 */
interface GeoJSONPolygon {
  coordinates: GeoJSONCoordinate[][]
  type: 'Polygon'
}

/**
 * Validate a GeoJSON coordinate pair [longitude, latitude]
 */
function isValidCoordinate(coord: unknown): coord is GeoJSONCoordinate {
  if (!Array.isArray(coord) || coord.length < 2) {
    return false
  }
  const [lng, lat] = coord
  return (
    typeof lng === 'number' &&
    typeof lat === 'number' &&
    !Number.isNaN(lng) &&
    !Number.isNaN(lat) &&
    Number.isFinite(lng) &&
    Number.isFinite(lat) &&
    lng >= -180 &&
    lng <= 180 &&
    lat >= -90 &&
    lat <= 90
  )
}

/**
 * Validate a GeoJSON Polygon
 */
function isValidPolygon(value: unknown): value is GeoJSONPolygon {
  if (typeof value !== 'object' || value === null) {
    return false
  }
  const geo = value as Record<string, unknown>
  if (geo.type !== 'Polygon' || !Array.isArray(geo.coordinates)) {
    return false
  }
  const ring = geo.coordinates[0]
  if (!Array.isArray(ring) || ring.length < 3) {
    return false
  }
  // Validate each coordinate in the ring
  return ring.every(isValidCoordinate)
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
        // Cast JSON fields to String for string comparisons only
        // Boolean and number comparisons should work directly
        if (path.startsWith('data.') && typeof value === 'string') {
          return `toString(${path}) = ${this.addParam(value)}`
        }
        return `${path} = ${this.addParam(value)}`

      case 'exists':
        return value === true ? `${path} IS NOT NULL` : `${path} IS NULL`

      case 'greater_than': {
        const gtParam = this.addDatetimeParam(path, value)
        return `${path} > ${gtParam}`
      }

      case 'greater_than_equal': {
        const gteParam = this.addDatetimeParam(path, value)
        return `${path} >= ${gteParam}`
      }

      case 'in': {
        if (!Array.isArray(value) || value.length === 0) {
          return '1=0'
        }
        const inValues = value.map((v) => this.addParam(v)).join(', ')
        // Cast JSON fields to String only for string values
        const hasStringValues = value.some((v) => typeof v === 'string')
        const inFieldExpr = path.startsWith('data.') && hasStringValues ? `toString(${path})` : path
        return `${inFieldExpr} IN (${inValues})`
      }

      case 'intersects':
      // falls through
      case 'within': {
        // GeoJSON format: { type: 'Polygon', coordinates: [[[lng, lat], ...]] }
        // Uses pointInPolygon to check if point is inside polygon
        if (!isValidPolygon(value)) {
          return '1=1'
        }
        const ring = value.coordinates[0]!
        // GeoJSON coordinates are [lng, lat] = (x, y), which is what pointInPolygon expects
        // Use parameterized values for each coordinate to prevent injection
        const polygonPoints = ring
          .map((coord) => {
            const lngParam = this.addParam(coord[0])
            const latParam = this.addParam(coord[1])
            return `(${lngParam}, ${latParam})`
          })
          .join(', ')
        // Point is stored as [lng, lat] array in JSON (GeoJSON convention)
        // Cast JSON to String first (Dynamic->String), then parse as Array
        const withinPointLon = `JSONExtractFloat(toString(${path}), 1)`
        const withinPointLat = `JSONExtractFloat(toString(${path}), 2)`
        return `pointInPolygon((${withinPointLon}, ${withinPointLat}), [${polygonPoints}]) = 1`
      }

      case 'less_than': {
        const ltParam = this.addDatetimeParam(path, value)
        return `${path} < ${ltParam}`
      }

      case 'less_than_equal': {
        const lteParam = this.addDatetimeParam(path, value)
        return `${path} <= ${lteParam}`
      }

      case 'like':
        if (typeof value !== 'string') {
          return '1=1'
        }
        // Use ILIKE for case-insensitive matching with parameterized value
        return `${path} ILIKE concat('%', ${this.addParam(value)}, '%')`
      case 'near': {
        // Format: 'lng, lat, maxDistance, minDistance' (GeoJSON convention: longitude first)
        // Uses geoDistance(lon1, lat1, lon2, lat2) which expects longitude first
        if (typeof value !== 'string') {
          return '1=1'
        }
        const parts = value.split(',').map((p) => parseFloat(p.trim()))
        if (parts.length < 3) {
          return '1=1'
        }
        const [centerLon, centerLat, maxDistance, minDistance = 0] = parts
        // Validate all numeric values
        if (
          !Number.isFinite(centerLon!) ||
          !Number.isFinite(centerLat!) ||
          !Number.isFinite(maxDistance!) ||
          !Number.isFinite(minDistance)
        ) {
          return '1=1'
        }
        // Validate coordinate ranges
        if (centerLon! < -180 || centerLon! > 180 || centerLat! < -90 || centerLat! > 90) {
          return '1=1'
        }
        // Validate distance values
        if (maxDistance! < 0 || minDistance < 0 || minDistance > maxDistance!) {
          return '1=1'
        }
        const lonParam = this.addParam(centerLon)
        const latParam = this.addParam(centerLat)
        const maxDistParam = this.addParam(maxDistance)
        const minDistParam = this.addParam(minDistance)
        // Point is stored as [lng, lat] array in JSON (GeoJSON convention)
        // Cast JSON to String first (Dynamic->String), then parse as Array
        const pointLon = `JSONExtractFloat(toString(${path}), 1)`
        const pointLat = `JSONExtractFloat(toString(${path}), 2)`
        // geoDistance expects: (lon1, lat1, lon2, lat2) - returns meters
        return `geoDistance(${lonParam}, ${latParam}, ${pointLon}, ${pointLat}) <= ${maxDistParam} AND geoDistance(${lonParam}, ${latParam}, ${pointLon}, ${pointLat}) >= ${minDistParam}`
      }

      case 'not_equals':
        if (value === null || value === undefined) {
          return `${path} IS NOT NULL`
        }
        // For JSON fields, NULL (missing field) should be considered "not equal" to any value
        // SQL's NULL != value returns NULL (falsy), so we need: (field IS NULL OR field != value)
        if (path.startsWith('data.')) {
          if (typeof value === 'string') {
            return `(${path} IS NULL OR toString(${path}) != ${this.addParam(value)})`
          }
          return `(${path} IS NULL OR ${path} != ${this.addParam(value)})`
        }
        return `${path} != ${this.addParam(value)}`

      case 'not_in': {
        if (!Array.isArray(value) || value.length === 0) {
          return '1=1'
        }
        const notInValues = value.map((v) => this.addParam(v)).join(', ')
        // Cast JSON fields to String only for string values
        const hasNotInStringValues = value.some((v) => typeof v === 'string')
        const notInFieldExpr =
          path.startsWith('data.') && hasNotInStringValues ? `toString(${path})` : path
        return `${notInFieldExpr} NOT IN (${notInValues})`
      }

      default:
        // Unknown operators return a true condition to avoid breaking queries
        // This is intentional - unknown operators are filtered out safely
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
      'within',
      'intersects',
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
   * Add a parameter for datetime comparison, converting ISO strings to DateTime64
   * for datetime columns (createdAt, updatedAt, deletedAt, v)
   */
  addDatetimeParam(field: string, value: unknown): string {
    const datetimeFields = ['createdAt', 'updatedAt', 'deletedAt', 'v']
    const isDatetimeField = datetimeFields.includes(field)

    // If comparing a datetime field with a string (ISO format), parse it
    if (isDatetimeField && typeof value === 'string') {
      const name = `p${this.paramCounter++}`
      this.params[name] = value
      return `parseDateTimeBestEffort({${name}:String})`
    }

    // Otherwise, use the regular parameter handling
    return this.addParam(value)
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
   * Build base WHERE without deletedAt filter (for use inside subqueries)
   */
  buildBaseWhereNoDeleted(ns: string, type: string): string {
    const nsParam = this.addNamedParam('ns', ns)
    const typeParam = this.addNamedParam('type', type)
    return `ns = ${nsParam} AND type = ${typeParam}`
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
