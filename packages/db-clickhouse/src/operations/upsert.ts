import type { Document, Upsert, UpsertArgs } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

/**
 * Upsert a document - create if not exists, update if exists
 *
 * Uses updateOne with upsert option for atomic behavior matching
 * MongoDB and Drizzle adapter patterns.
 */
export const upsert: Upsert = async function upsert(
  this: ClickHouseAdapter,
  args: UpsertArgs,
): Promise<Document> {
  const { collection, data, joins, locale, req, returning, select, where } = args

  return this.updateOne({
    collection,
    data,
    joins,
    locale,
    options: { upsert: true },
    req,
    returning,
    select,
    where,
  })
}
