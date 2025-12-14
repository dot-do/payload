import type { PaginatedDocs, QueryDrafts, QueryDraftsArgs, TypeWithID } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

/**
 * Query drafts - for ClickHouse we filter versions with latest=true
 */
export const queryDrafts: QueryDrafts = async function queryDrafts<T = TypeWithID>(
  this: ClickHouseAdapter,
  args: QueryDraftsArgs,
): Promise<PaginatedDocs<T>> {
  const { collection, limit = 10, page = 1, pagination = true, sort, where } = args

  // Use findVersions with latest: true filter
  const result = await this.findVersions({
    collection,
    limit,
    page,
    pagination,
    sort,
    where: {
      ...where,
      latest: { equals: true },
    },
  })

  // Transform version docs to regular docs
  const docs = result.docs.map((versionDoc) => ({
    ...versionDoc.version,
    id: versionDoc.parent,
    _status: 'draft',
  })) as T[]

  return {
    ...result,
    docs,
  }
}
