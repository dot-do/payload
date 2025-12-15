import type { ClickHouseAdapter, SyncToSearchArgs } from '../types.js'

import { generateUlid, generateVersion } from '../utilities/generateId.js'

/**
 * Convert a document to YAML-like string representation for full-text search
 */
function docToText(doc: Record<string, unknown>, indent = 0): string {
  const lines: string[] = []
  const prefix = '  '.repeat(indent)

  for (const [key, value] of Object.entries(doc)) {
    if (value === null || value === undefined) {
      continue
    }

    if (Array.isArray(value)) {
      lines.push(`${prefix}${key}:`)
      for (const item of value) {
        if (typeof item === 'object' && item !== null) {
          lines.push(`${prefix}  -`)
          lines.push(docToText(item as Record<string, unknown>, indent + 2))
        } else {
          lines.push(`${prefix}  - ${String(item)}`)
        }
      }
    } else if (typeof value === 'object') {
      lines.push(`${prefix}${key}:`)
      lines.push(docToText(value as Record<string, unknown>, indent + 1))
    } else {
      lines.push(`${prefix}${key}: ${String(value)}`)
    }
  }

  return lines.join('\n')
}

export async function syncToSearch(
  this: ClickHouseAdapter,
  args: SyncToSearchArgs,
): Promise<string> {
  const { chunkIndex = 0, collection, doc } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const docId = String(doc.id)
  const id = generateUlid()
  const now = generateVersion()
  const text = docToText(doc)

  // Create empty embedding array with correct dimensions
  const emptyEmbedding = new Array(this.embeddingDimensions).fill(0)

  const query = `
    INSERT INTO search (id, ns, collection, docId, chunkIndex, text, embedding, status, createdAt, updatedAt)
    VALUES (
      {id:String},
      {ns:String},
      {collection:String},
      {docId:String},
      {chunkIndex:UInt16},
      {text:String},
      {embedding:Array(Float32)},
      'pending',
      fromUnixTimestamp64Milli({createdAt:Int64}),
      fromUnixTimestamp64Milli({updatedAt:Int64})
    )
  `

  await this.clickhouse.command({
    query,
    query_params: {
      id,
      chunkIndex,
      collection,
      createdAt: now,
      docId,
      embedding: emptyEmbedding,
      ns: this.namespace,
      text,
      updatedAt: now,
    },
  })

  return id
}
