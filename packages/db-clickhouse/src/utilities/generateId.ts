import { ulid } from 'ulid'

/**
 * Alphabet for nanoid-style IDs (URL-safe)
 */
const ALPHABET = 'useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict'

/**
 * Monotonic counter for version timestamps to prevent collisions
 * when multiple operations occur within the same millisecond
 */
let lastVersion = 0

/**
 * Generate a nanoid-style ID (21 characters, URL-safe)
 * Uses crypto.getRandomValues() which is available in all modern runtimes including Cloudflare Workers
 */
function generateNanoid(size = 21): string {
  const bytes = new Uint8Array(size)
  crypto.getRandomValues(bytes)
  let id = ''
  for (let i = 0; i < size; i++) {
    id += ALPHABET[bytes[i]! & 63]
  }
  return id
}

/**
 * Generate a UUID using crypto.randomUUID()
 * Available in all modern runtimes including Cloudflare Workers
 */
function generateUUID(): string {
  return crypto.randomUUID()
}

/**
 * Generate a unique ID for document IDs
 *
 * @param idType - 'text' for nanoid-style (default), 'uuid' for standard UUID
 */
export function generateId(idType: 'text' | 'uuid' = 'text'): string {
  return idType === 'uuid' ? generateUUID() : generateNanoid()
}

/**
 * Generate a monotonically increasing version timestamp
 * Uses Date.now() as the base but ensures each call returns a unique value
 * even when multiple operations occur within the same millisecond
 *
 * @returns Milliseconds since epoch, guaranteed to be greater than the last call
 */
export function generateVersion(): number {
  const now = Date.now()
  lastVersion = Math.max(now, lastVersion + 1)
  return lastVersion
}

/**
 * Generate a ULID (Universally Unique Lexicographically Sortable Identifier)
 * Time-ordered and sortable, ideal for event logs
 */
export function generateUlid(): string {
  return ulid()
}
