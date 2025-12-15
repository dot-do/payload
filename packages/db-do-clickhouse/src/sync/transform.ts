import type { OplogEntry } from '../types.js'

/**
 * ClickHouse insert data format
 */
export interface ClickHouseInsertRow {
  createdAt: number
  createdBy: null | string
  data: string
  deletedAt: null | number
  deletedBy: null | string
  id: string
  ns: string
  tenant: string
  title: string
  type: string
  updatedAt: number
  updatedBy: null | string
  v: number
}

/**
 * Extract title from document data
 */
export function extractTitle(data: Record<string, unknown>, id: string): string {
  if (typeof data.title === 'string') {
    return data.title
  }
  if (typeof data.name === 'string') {
    return data.name
  }
  if (typeof data.label === 'string') {
    return data.label
  }
  if (typeof data.email === 'string') {
    return data.email
  }
  if (typeof data.slug === 'string') {
    return data.slug
  }
  return id
}

/**
 * Transform an oplog entry to ClickHouse insert format
 */
export function oplogEntryToClickHouseRow(
  entry: OplogEntry,
  namespace: string,
  tenant: string = '',
): ClickHouseInsertRow | null {
  // For delete operations, we soft delete by setting deletedAt
  if (entry.op === 'delete') {
    return {
      id: entry.doc_id,
      type: entry.collection,
      createdAt: entry.timestamp,
      createdBy: null,
      data: '{}',
      deletedAt: entry.timestamp,
      deletedBy: null,
      ns: namespace,
      tenant,
      title: '',
      updatedAt: entry.timestamp,
      updatedBy: null,
      v: entry.timestamp,
    }
  }

  // For insert/update, we need the data
  if (!entry.data) {
    return null
  }

  const { id: _id, createdAt, updatedAt, ...restData } = entry.data

  return {
    id: entry.doc_id,
    type: entry.collection,
    createdAt:
      typeof createdAt === 'string'
        ? new Date(createdAt).getTime()
        : typeof createdAt === 'number'
          ? createdAt
          : entry.timestamp,
    createdBy: (entry.data.createdBy as null | string) ?? null,
    data: JSON.stringify(restData),
    deletedAt: null,
    deletedBy: null,
    ns: namespace,
    tenant,
    title: extractTitle(entry.data, entry.doc_id),
    updatedAt:
      typeof updatedAt === 'string'
        ? new Date(updatedAt).getTime()
        : typeof updatedAt === 'number'
          ? updatedAt
          : entry.timestamp,
    updatedBy: (entry.data.updatedBy as null | string) ?? null,
    v: entry.timestamp,
  }
}

/**
 * Transform multiple oplog entries to ClickHouse format
 * Groups by document ID to get the latest state for each document
 */
export function oplogEntriesToClickHouseRows(
  entries: OplogEntry[],
  namespace: string,
  tenant: string = '',
): ClickHouseInsertRow[] {
  // Group entries by doc_id to deduplicate
  const latestByDoc = new Map<string, OplogEntry>()

  for (const entry of entries) {
    const key = `${entry.collection}:${entry.doc_id}`
    const existing = latestByDoc.get(key)

    // Keep the latest entry (highest seq or timestamp)
    if (!existing || (entry.seq && existing.seq && entry.seq > existing.seq)) {
      latestByDoc.set(key, entry)
    }
  }

  const rows: ClickHouseInsertRow[] = []

  for (const entry of latestByDoc.values()) {
    const row = oplogEntryToClickHouseRow(entry, namespace, tenant)
    if (row) {
      rows.push(row)
    }
  }

  return rows
}
