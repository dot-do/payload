import type { CollectionAfterChangeHook } from 'payload'

import type { SearchCollectionConfig } from '../types.js'

interface SyncWithSearchArgs {
  chunkOverlap?: number
  chunkSize?: number
  collectionSlug: string
  searchConfig: SearchCollectionConfig
}

/**
 * Extract text from a document based on configured fields
 */
const extractText = (doc: Record<string, unknown>, fields: string[]): string => {
  const parts: string[] = []

  for (const fieldPath of fields) {
    const value = fieldPath.split('.').reduce<unknown>((obj, key) => {
      if (obj && typeof obj === 'object' && key in obj) {
        return (obj as Record<string, unknown>)[key]
      }
      return undefined
    }, doc)

    if (typeof value === 'string' && value.trim()) {
      parts.push(value.trim())
    }
  }

  return parts.join('\n\n')
}

/**
 * Chunk text into smaller pieces for embedding
 */
const chunkText = (text: string, chunkSize: number, overlap: number): string[] => {
  if (text.length <= chunkSize) {
    return [text]
  }

  const chunks: string[] = []
  let start = 0

  while (start < text.length) {
    const end = Math.min(start + chunkSize, text.length)
    chunks.push(text.slice(start, end))
    start = end - overlap
    if (start >= text.length - overlap) {
      break
    }
  }

  return chunks
}

export const syncWithSearch =
  ({
    chunkOverlap = 100,
    chunkSize = 1000,
    collectionSlug,
    searchConfig,
  }: SyncWithSearchArgs): CollectionAfterChangeHook =>
  async ({ doc, req }) => {
    const { payload } = req

    // Check if db adapter has syncToSearch
    if (typeof payload.db.syncToSearch !== 'function') {
      payload.logger.warn('syncToSearch not available on database adapter - skipping search sync')
      return doc
    }

    try {
      const text = extractText(doc, searchConfig.fields)

      if (!text) {
        return doc
      }

      const chunks = chunkText(text, chunkSize, chunkOverlap)

      for (let i = 0; i < chunks.length; i++) {
        await payload.db.syncToSearch({
          chunkIndex: i,
          collection: collectionSlug,
          doc: {
            ...doc,
            _extractedText: chunks[i],
          },
        })
      }
    } catch (error) {
      payload.logger.error({
        err: error,
        msg: `Failed to sync ${collectionSlug}/${doc.id} to search`,
      })
    }

    return doc
  }
