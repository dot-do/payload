import type { Document, Upsert, UpsertArgs } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

/**
 * Upsert a document - create if not exists, update if exists
 */
export const upsert: Upsert = async function upsert(
  this: ClickHouseAdapter,
  args: UpsertArgs,
): Promise<Document> {
  const { collection, data, req, where } = args

  // Try to find existing document
  const existing = await this.findOne({
    collection,
    req,
    where,
  })

  if (existing) {
    // Update existing document
    return this.updateOne({
      collection,
      data,
      req,
      where: { id: { equals: existing.id } },
    })
  } else {
    // Create new document
    return this.create({
      collection,
      data,
      req,
    })
  }
}
