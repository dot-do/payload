import type { CollectionBeforeDeleteHook } from 'payload'

interface DeleteFromSearchArgs {
  collectionSlug: string
}

// Type for ClickHouse adapter methods (not all adapters have these)
interface ClickHouseDb {
  execute: (args: { query: string; query_params: Record<string, unknown> }) => Promise<unknown>
  namespace: string
}

export const deleteFromSearch =
  ({ collectionSlug }: DeleteFromSearchArgs): CollectionBeforeDeleteHook =>
  async ({ id, req }) => {
    const { payload } = req
    const db = payload.db as unknown as ClickHouseDb

    // Check if db adapter has search delete capability
    if (typeof db.execute !== 'function') {
      return
    }

    try {
      // Delete all search entries for this document
      await db.execute({
        query: `
          ALTER TABLE search
          DELETE WHERE collection = {collection:String} AND docId = {docId:String} AND ns = {ns:String}
        `,
        query_params: {
          collection: collectionSlug,
          docId: String(id),
          ns: db.namespace,
        },
      })
    } catch (error) {
      payload.logger.error({
        err: error,
        msg: `Failed to delete ${collectionSlug}/${id} from search`,
      })
    }
  }
