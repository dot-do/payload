import type { JoinQuery, SanitizedJoins } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

export type ResolveJoinsArgs = {
  adapter: ClickHouseAdapter
  collectionSlug: string
  docs: Record<string, unknown>[]
  joins?: JoinQuery
  locale?: string
  versions?: boolean
}

type SanitizedJoin = SanitizedJoins[string][number]

interface RelationshipMatch {
  fromId: string
  fromType: string
  toId: string
}

async function queryRelationships(
  adapter: ClickHouseAdapter,
  options: {
    fromField: string
    fromTypes: string[]
    locale?: null | string
    toIds: string[]
    toType: string
  },
): Promise<RelationshipMatch[]> {
  const { fromField, fromTypes, locale, toIds, toType } = options

  if (toIds.length === 0 || fromTypes.length === 0) {
    return []
  }

  // Build IN clause for toIds
  const toIdParams: Record<string, string> = {}
  const toIdPlaceholders: string[] = []
  toIds.forEach((id, i) => {
    const paramName = `toId_${i}`
    toIdParams[paramName] = id
    toIdPlaceholders.push(`{${paramName}:String}`)
  })

  // Build IN clause for fromTypes
  const fromTypeParams: Record<string, string> = {}
  const fromTypePlaceholders: string[] = []
  fromTypes.forEach((type, i) => {
    const paramName = `fromType_${i}`
    fromTypeParams[paramName] = type
    fromTypePlaceholders.push(`{${paramName}:String}`)
  })

  const localeCondition = locale ? `AND locale = {locale:String}` : ''

  const query = `
    SELECT DISTINCT fromId, fromType, toId
    FROM (
      SELECT *,
        row_number() OVER (
          PARTITION BY ns, fromType, fromId, fromField, toType, toId, position
          ORDER BY v DESC
        ) as _rn
      FROM ${adapter.table}_relationships
      WHERE ns = {ns:String}
        AND toType = {toType:String}
        AND toId IN (${toIdPlaceholders.join(', ')})
        AND fromType IN (${fromTypePlaceholders.join(', ')})
        AND fromField = {fromField:String}
        ${localeCondition}
    )
    WHERE _rn = 1 AND deletedAt IS NULL
  `

  const params: Record<string, string> = {
    fromField,
    ns: adapter.namespace,
    toType,
    ...toIdParams,
    ...fromTypeParams,
  }

  if (locale) {
    params.locale = locale
  }

  const result = await adapter.clickhouse!.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  return result.json<RelationshipMatch>()
}

/**
 * Resolves join relationships for a collection of documents.
 * Queries the relationships table to find documents that reference
 * the parent documents, then attaches them with pagination metadata.
 */
export async function resolveJoins({
  adapter,
  collectionSlug,
  docs,
  joins,
  locale,
  versions = false,
}: ResolveJoinsArgs): Promise<void> {
  // Early return if no joins requested or no documents
  if (!joins || docs.length === 0) {
    return
  }

  const collectionConfig = adapter.payload.collections[collectionSlug]?.config
  if (!collectionConfig) {
    return
  }

  // Build map of join paths to their configurations
  const joinMap: Record<string, { targetCollection: string } & SanitizedJoin> = {}

  // Add regular joins (keyed by target collection)
  for (const [targetCollection, joinList] of Object.entries(collectionConfig.joins || {})) {
    for (const join of joinList) {
      joinMap[join.joinPath] = { ...join, targetCollection }
    }
  }

  // Add polymorphic joins
  for (const join of collectionConfig.polymorphicJoins || []) {
    const targetCollection = Array.isArray(join.field.collection)
      ? join.field.collection[0] || ''
      : join.field.collection || ''
    joinMap[join.joinPath] = { ...join, targetCollection }
  }

  // Extract parent document IDs
  const parentIds = docs.map((doc) => (versions ? String(doc.parent ?? doc.id) : String(doc.id)))

  // Process each requested join
  const joinPromises = Object.entries(joins).map(async ([joinPath, joinQuery]) => {
    if (!joinQuery) {
      return
    }

    const joinDef = joinMap[joinPath]
    if (!joinDef) {
      return
    }

    // Get target collections (array for polymorphic)
    const targetCollections = Array.isArray(joinDef.field.collection)
      ? joinDef.field.collection
      : [joinDef.field.collection]

    // Query relationships table
    const relationships = await queryRelationships(adapter, {
      fromField: joinDef.field.on,
      fromTypes: targetCollections,
      locale: locale ?? null,
      toIds: parentIds,
      toType: collectionSlug,
    })

    if (relationships.length === 0) {
      // Attach empty results to all docs
      for (const doc of docs) {
        doc[joinDef.field.name] = {
          docs: [],
          hasNextPage: false,
        }
      }
      return
    }

    // Group relationships by parent (toId)
    const relsByParent = new Map<string, RelationshipMatch[]>()
    for (const rel of relationships) {
      const existing = relsByParent.get(rel.toId) || []
      existing.push(rel)
      relsByParent.set(rel.toId, existing)
    }

    // Attach to documents (basic - without fetching full docs yet)
    const isPolymorphic = Array.isArray(joinDef.field.collection)
    for (const doc of docs) {
      const docId = versions ? String(doc.parent ?? doc.id) : String(doc.id)
      const rels = relsByParent.get(docId) || []

      // Apply pagination
      const limit = joinQuery.limit ?? joinDef.field.defaultLimit ?? 10
      const page = joinQuery.page ?? 1
      const skip = (page - 1) * limit
      const sliced = limit === 0 ? rels : rels.slice(skip, skip + limit)
      const hasNextPage = limit !== 0 && rels.length > skip + limit

      doc[joinDef.field.name] = {
        docs: sliced.map((rel) =>
          isPolymorphic ? { relationTo: rel.fromType, value: rel.fromId } : rel.fromId,
        ),
        hasNextPage,
        ...(joinQuery.count ? { totalDocs: rels.length } : {}),
      }
    }
  })

  await Promise.all(joinPromises)
}
