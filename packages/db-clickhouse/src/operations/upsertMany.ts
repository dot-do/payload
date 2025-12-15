import type { Document } from 'payload'

import type { ClickHouseAdapter, UpsertManyArgs } from '../types.js'

/**
 * Bulk upsert operation - create or update multiple documents
 *
 * Each document in the array is upserted independently using the
 * provided where clause to find existing documents.
 *
 * @example
 * ```ts
 * const docs = await payload.db.upsertMany({
 *   collection: 'posts',
 *   docs: [
 *     { data: { title: 'Post 1', slug: 'post-1' }, where: { slug: { equals: 'post-1' } } },
 *     { data: { title: 'Post 2', slug: 'post-2' }, where: { slug: { equals: 'post-2' } } },
 *   ]
 * })
 * ```
 */
export async function upsertMany(
  this: ClickHouseAdapter,
  args: UpsertManyArgs,
): Promise<Document[]> {
  const { collection, docs, req } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  // Process all upserts in parallel for better performance
  const results = await Promise.all(
    docs.map((doc) =>
      this.upsert({
        collection,
        data: doc.data,
        req,
        where: doc.where,
      }),
    ),
  )

  return results
}
