/**
 * Alphabet for nanoid-style IDs (URL-safe)
 */
const ALPHABET = 'useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict'

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
 * Generate a version timestamp using Date.now()
 * Returns milliseconds since epoch
 */
export function generateVersion(): number {
  return Date.now()
}
