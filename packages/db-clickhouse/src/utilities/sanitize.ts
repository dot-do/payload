/**
 * Escape a string value for use in ClickHouse SQL
 * Prevents SQL injection by escaping single quotes and backslashes
 */
export function escapeString(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/'/g, "\\'")
}

/**
 * Format a value for SQL insertion based on its type
 */
export function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'NULL'
  }

  if (typeof value === 'string') {
    return `'${escapeString(value)}'`
  }

  if (typeof value === 'number') {
    if (Number.isNaN(value) || !Number.isFinite(value)) {
      return 'NULL'
    }
    return String(value)
  }

  if (typeof value === 'boolean') {
    return value ? '1' : '0'
  }

  if (value instanceof Date) {
    return `fromUnixTimestamp64Milli(${value.getTime()})`
  }

  if (Array.isArray(value)) {
    const formatted = value.map(formatValue).join(', ')
    return `[${formatted}]`
  }

  if (typeof value === 'object') {
    return `'${escapeString(JSON.stringify(value))}'`
  }

  return `'${escapeString(String(value))}'`
}

/**
 * Format a timestamp (milliseconds) for DateTime64(3)
 */
export function formatTimestamp(ms: number): string {
  return `fromUnixTimestamp64Milli(${ms})`
}

/**
 * Escape an identifier (table name, column name, etc.)
 */
export function escapeIdentifier(identifier: string): string {
  // ClickHouse uses backticks for identifiers with special characters
  return `\`${identifier.replace(/`/g, '``')}\``
}

/**
 * Validate that a collection slug is safe to use in queries
 */
export function validateCollectionSlug(slug: string): boolean {
  // Only allow alphanumeric, underscores, and hyphens
  return /^[\w-]+$/.test(slug)
}

/**
 * Validate and return the collection slug, throwing if invalid
 */
export function assertValidSlug(slug: string, context: string): string {
  if (!validateCollectionSlug(slug)) {
    throw new Error(
      `Invalid ${context} slug '${slug}'. Slugs must contain only alphanumeric characters, underscores, and hyphens.`,
    )
  }
  return slug
}

/**
 * Sanitize a field path for use in JSON queries
 * Converts dot notation to ClickHouse JSON path syntax
 */
export function sanitizeFieldPath(path: string): string {
  // Remove any characters that could be used for injection
  const cleaned = path.replace(/[^\w.[\]]/g, '')
  return cleaned
}
