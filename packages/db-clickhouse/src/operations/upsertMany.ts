import type { Document } from 'payload'

import type { ClickHouseAdapter, UpsertManyArgs } from '../types.js'

import { generateId, generateVersion } from '../utilities/generateId.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { extractTitle, stripSensitiveFields } from '../utilities/transform.js'

/**
 * Bulk upsert operation - insert multiple documents in a single batch
 *
 * Uses ClickHouse's native batch insert for optimal performance.
 * Documents with an existing `id` will create a new version (ReplacingMergeTree handles dedup).
 * Documents without an `id` will have one generated.
 *
 * @example
 * ```ts
 * const docs = await payload.db.upsertMany({
 *   collection: 'posts',
 *   docs: [
 *     { data: { title: 'Post 1', slug: 'post-1' } },
 *     { data: { id: 'existing-id', title: 'Updated Post' } },
 *   ]
 * })
 * ```
 */
export async function upsertMany(
  this: ClickHouseAdapter,
  args: UpsertManyArgs,
): Promise<Document[]> {
  const { collection: collectionSlug, docs, req } = args

  assertValidSlug(collectionSlug, 'collection')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  if (docs.length === 0) {
    return []
  }

  const collection = this.payload.collections[collectionSlug]
  if (!collection) {
    throw new Error(`Collection '${collectionSlug}' not found`)
  }

  const now = generateVersion()
  const hasTimestamps = collection.config.timestamps !== false
  const titleField = collection.config.admin?.useAsTitle
  const userId = req?.user?.id ? String(req.user.id) : null

  const results: Document[] = []
  const rows: Array<Record<string, unknown>> = []

  for (const doc of docs) {
    const {
      id: providedId,
      createdAt: userCreatedAt,
      updatedAt: userUpdatedAt,
      ...rawData
    } = doc.data
    const id = providedId ? String(providedId) : generateId(this.idType)
    const docData = stripSensitiveFields(rawData)
    const title = extractTitle(docData, titleField, id)

    const createdAtValue = userCreatedAt ? new Date(userCreatedAt as string).getTime() : now
    const updatedAtValue = userUpdatedAt ? new Date(userUpdatedAt as string).getTime() : now

    rows.push({
      id,
      type: collectionSlug,
      createdAt: new Date(hasTimestamps ? createdAtValue : now),
      createdBy: userId,
      data: docData,
      deletedAt: null,
      deletedBy: null,
      ns: this.namespace,
      title,
      updatedAt: new Date(hasTimestamps ? updatedAtValue : now),
      updatedBy: userId,
      v: new Date(now),
    })

    const result: Document = { id, ...docData }
    if (hasTimestamps) {
      result.createdAt = new Date(createdAtValue).toISOString()
      result.updatedAt = new Date(updatedAtValue).toISOString()
    }
    results.push(result)
  }

  await this.clickhouse.insert({
    format: 'JSONEachRow',
    table: this.table,
    values: rows,
  })

  return results
}
