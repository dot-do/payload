/**
 * Top-level fields that are columns, not in JSON
 */
const TOP_LEVEL_FIELDS = [
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

/**
 * Get the ClickHouse field path for a Payload field
 * Handles both top-level fields and nested JSON paths
 */
function getFieldPath(field: string): string {
  // Sanitize field path - only allow alphanumeric, underscores, dots, and brackets
  const sanitized = field.replace(/[^\w.[\]]/g, '')

  if (TOP_LEVEL_FIELDS.includes(sanitized)) {
    return sanitized
  }

  // Handle version.X paths - version data is stored directly in data
  if (sanitized.startsWith('version.')) {
    const versionField = sanitized.slice('version.'.length)
    return `data.${versionField}`
  }

  // For nested fields, use ClickHouse JSON path syntax
  return `data.${sanitized}`
}

/**
 * Parse a Payload sort string into ClickHouse ORDER BY clause
 *
 * Payload sort format: "field" for ascending, "-field" for descending
 * Can also be comma-separated for multiple fields
 */
export function buildOrderBy(sort: string | undefined): string {
  if (!sort) {
    // Default sort by createdAt descending
    return 'ORDER BY createdAt DESC'
  }

  const fields = sort.split(',').map((s) => s.trim())
  const orderParts: string[] = []

  for (const field of fields) {
    if (!field) {continue}

    let direction = 'ASC'
    let fieldName = field

    if (field.startsWith('-')) {
      direction = 'DESC'
      fieldName = field.slice(1)
    }

    const path = getFieldPath(fieldName)
    orderParts.push(`${path} ${direction}`)
  }

  if (orderParts.length === 0) {
    return 'ORDER BY createdAt DESC'
  }

  return `ORDER BY ${orderParts.join(', ')}`
}

/**
 * Build LIMIT and OFFSET clause for pagination
 */
export function buildLimitOffset(limit: number, page: number): string {
  if (limit <= 0) {
    return '' // No limit
  }

  const offset = (page - 1) * limit
  return `LIMIT ${limit} OFFSET ${offset}`
}
