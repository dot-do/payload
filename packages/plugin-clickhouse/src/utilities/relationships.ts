// packages/plugin-clickhouse/src/utilities/relationships.ts
import type { Payload } from 'payload'

export interface GetLinksArgs {
  collection: string
  id: string
}

export interface LinkResult {
  fromField: string
  fromId: string
  fromType: string
  locale: null | string
  position: number
  toId: string
  toType: string
}

export interface TraverseGraphArgs {
  collection: string
  depth?: number
  direction?: 'both' | 'incoming' | 'outgoing'
  id: string
}

export interface GraphNode {
  collection: string
  depth: number
  id: string
  links: LinkResult[]
}

/**
 * Find all documents that link TO a specific document
 */
export const createGetIncomingLinks = (payload: Payload) => {
  return async ({ id, collection }: GetLinksArgs): Promise<LinkResult[]> => {
    if (typeof payload.db.execute !== 'function') {
      return []
    }

    const results = await payload.db.execute<LinkResult>({
      query: `
        SELECT fromType, fromId, fromField, toType, toId, position, locale
        FROM relationships
        WHERE toType = {toType:String}
          AND toId = {toId:String}
          AND ns = {ns:String}
          AND deletedAt IS NULL
        ORDER BY fromType, fromId
      `,
      query_params: {
        ns: payload.db.namespace,
        toId: id,
        toType: collection,
      },
    })

    return results
  }
}

/**
 * Find all documents that a specific document links TO
 */
export const createGetOutgoingLinks = (payload: Payload) => {
  return async ({ id, collection }: GetLinksArgs): Promise<LinkResult[]> => {
    if (typeof payload.db.execute !== 'function') {
      return []
    }

    const results = await payload.db.execute<LinkResult>({
      query: `
        SELECT fromType, fromId, fromField, toType, toId, position, locale
        FROM relationships
        WHERE fromType = {fromType:String}
          AND fromId = {fromId:String}
          AND ns = {ns:String}
          AND deletedAt IS NULL
        ORDER BY toType, toId
      `,
      query_params: {
        fromId: id,
        fromType: collection,
        ns: payload.db.namespace,
      },
    })

    return results
  }
}

/**
 * Find orphaned references (links to documents that no longer exist)
 */
export const createFindOrphanedLinks = (payload: Payload) => {
  return async (args?: { collection?: string }): Promise<LinkResult[]> => {
    if (typeof payload.db.execute !== 'function') {
      return []
    }

    const collectionFilter = args?.collection ? 'AND r.fromType = {collection:String}' : ''

    const results = await payload.db.execute<LinkResult>({
      query: `
        SELECT r.fromType, r.fromId, r.fromField, r.toType, r.toId, r.position, r.locale
        FROM relationships r
        LEFT JOIN data d ON r.toType = d.type AND r.toId = d.id AND r.ns = d.ns
        WHERE r.ns = {ns:String}
          AND r.deletedAt IS NULL
          AND (d.id IS NULL OR d.deletedAt IS NOT NULL)
          ${collectionFilter}
        ORDER BY r.fromType, r.fromId
      `,
      query_params: {
        collection: args?.collection || '',
        ns: payload.db.namespace,
      },
    })

    return results
  }
}

/**
 * Traverse the document graph to find connected documents
 */
export const createTraverseGraph = (payload: Payload) => {
  return async ({
    id,
    collection,
    depth = 2,
    direction = 'both',
  }: TraverseGraphArgs): Promise<GraphNode[]> => {
    const visited = new Set<string>()
    const results: GraphNode[] = []
    const getIncoming = createGetIncomingLinks(payload)
    const getOutgoing = createGetOutgoingLinks(payload)

    const traverse = async (col: string, docId: string, currentDepth: number) => {
      const key = `${col}:${docId}`
      if (visited.has(key) || currentDepth > depth) {
        return
      }
      visited.add(key)

      const links: LinkResult[] = []

      if (direction === 'incoming' || direction === 'both') {
        const incoming = await getIncoming({ id: docId, collection: col })
        links.push(...incoming)
      }

      if (direction === 'outgoing' || direction === 'both') {
        const outgoing = await getOutgoing({ id: docId, collection: col })
        links.push(...outgoing)
      }

      results.push({
        id: docId,
        collection: col,
        depth: currentDepth,
        links,
      })

      // Continue traversal
      for (const link of links) {
        if (direction === 'incoming' || direction === 'both') {
          await traverse(link.fromType, link.fromId, currentDepth + 1)
        }
        if (direction === 'outgoing' || direction === 'both') {
          await traverse(link.toType, link.toId, currentDepth + 1)
        }
      }
    }

    await traverse(collection, id, 0)
    return results
  }
}
