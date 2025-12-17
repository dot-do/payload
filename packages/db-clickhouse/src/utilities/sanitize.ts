/**
 * Validate that a namespace is safe to use in queries
 * Allows alphanumeric, underscores, hyphens, and dots (for domain names)
 */
export function validateNamespace(namespace: string): boolean {
  // Allow alphanumeric, underscores, hyphens, and dots
  // Must start with alphanumeric or underscore
  return /^\w[\w.-]*$/.test(namespace)
}

/**
 * Validate and return the namespace, throwing if invalid
 */
export function assertValidNamespace(namespace: string): string {
  if (!validateNamespace(namespace)) {
    throw new Error(
      `Invalid namespace '${namespace}'. Namespaces must start with a letter, number, or underscore and contain only alphanumeric characters, underscores, hyphens, and dots.`,
    )
  }
  return namespace
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
