import type { Field } from 'payload'

import { fieldAffectsData } from 'payload/shared'

import type { RelationshipRow } from '../types.js'

export interface ExtractRelationshipsOptions {
  fromId: string
  fromType: string
  locale?: string
  ns: string
  v: number
}

/**
 * Extract relationships from document data based on field definitions
 */
export function extractRelationships(
  data: Record<string, unknown>,
  fields: Field[],
  options: ExtractRelationshipsOptions,
): RelationshipRow[] {
  const relationships: RelationshipRow[] = []

  for (const field of fields) {
    if (!fieldAffectsData(field)) {
      continue
    }

    if (field.type === 'relationship' || field.type === 'upload') {
      const value = data[field.name]
      if (value === null || value === undefined) {
        continue
      }

      const relationTos = Array.isArray(field.relationTo) ? field.relationTo : [field.relationTo]

      if (field.hasMany && Array.isArray(value)) {
        value.forEach((item, position) => {
          const rel = parseRelationshipValue(item, relationTos, field.name, position, options)
          if (rel) {
            relationships.push(rel)
          }
        })
      } else {
        const rel = parseRelationshipValue(value, relationTos, field.name, 0, options)
        if (rel) {
          relationships.push(rel)
        }
      }
    }

    // Recurse into group fields
    if (field.type === 'group' && field.fields) {
      const groupData = data[field.name] as Record<string, unknown> | undefined
      if (groupData) {
        const nested = extractRelationships(groupData, field.fields, options)
        relationships.push(...nested)
      }
    }

    // Recurse into array fields
    if (field.type === 'array' && field.fields) {
      const arrayData = data[field.name] as Record<string, unknown>[] | undefined
      if (Array.isArray(arrayData)) {
        for (const item of arrayData) {
          const nested = extractRelationships(item, field.fields, options)
          relationships.push(...nested)
        }
      }
    }

    // Recurse into blocks
    if (field.type === 'blocks' && field.blocks) {
      const blocksData = data[field.name] as
        | Array<{ blockType: string } & Record<string, unknown>>
        | undefined
      if (Array.isArray(blocksData)) {
        for (const block of blocksData) {
          const blockConfig = field.blocks.find((b) => b.slug === block.blockType)
          if (blockConfig?.fields) {
            const nested = extractRelationships(block, blockConfig.fields, options)
            relationships.push(...nested)
          }
        }
      }
    }

    // Recurse into tabs
    if (field.type === 'tabs' && field.tabs) {
      for (const tab of field.tabs) {
        if ('fields' in tab && tab.fields) {
          // Named tab - data is nested under tab name
          if ('name' in tab && tab.name) {
            const tabData = data[tab.name] as Record<string, unknown> | undefined
            if (tabData) {
              const nested = extractRelationships(tabData, tab.fields, options)
              relationships.push(...nested)
            }
          } else {
            // Unnamed tab - fields are at root level
            const nested = extractRelationships(data, tab.fields, options)
            relationships.push(...nested)
          }
        }
      }
    }
  }

  return relationships
}

/**
 * Parse a relationship value (handles both simple IDs and polymorphic { relationTo, value })
 */
function parseRelationshipValue(
  value: unknown,
  relationTos: string[],
  fromField: string,
  position: number,
  options: ExtractRelationshipsOptions,
): null | RelationshipRow {
  if (value === null || value === undefined) {
    return null
  }

  let toType: string
  let toId: string

  // Polymorphic relationship: { relationTo: 'collection', value: 'id' }
  if (typeof value === 'object' && value !== null && 'relationTo' in value && 'value' in value) {
    const polymorphic = value as { relationTo: string; value: unknown }
    toType = polymorphic.relationTo
    toId = String(polymorphic.value)
  } else {
    // Simple relationship: just the ID
    toType = relationTos[0]!
    toId = String(value)
  }

  // Skip if toId is empty or invalid
  if (!toId || toId === 'undefined' || toId === 'null') {
    return null
  }

  return {
    ...options,
    deletedAt: null,
    fromField,
    locale: options.locale ?? null,
    position,
    toId,
    toType,
  }
}
