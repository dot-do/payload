import type { CollectionBeforeDeleteHook } from 'payload'

interface DeleteFromSearchArgs {
  collectionSlug: string
}

export const deleteFromSearch =
  ({ collectionSlug }: DeleteFromSearchArgs): CollectionBeforeDeleteHook =>
  async ({ id, req }) => {
    const { payload } = req

    // Check if db adapter has search delete capability
    if (typeof payload.db.execute !== 'function') {
      return
    }

    try {
      // Delete all search entries for this document
      await payload.db.execute({
        query: `
          ALTER TABLE search
          DELETE WHERE collection = {collection:String} AND docId = {docId:String} AND ns = {ns:String}
        `,
        query_params: {
          collection: collectionSlug,
          docId: String(id),
          ns: payload.db.namespace,
        },
      })
    } catch (error) {
      payload.logger.error({
        err: error,
        msg: `Failed to delete ${collectionSlug}/${id} from search`,
      })
    }
  }
