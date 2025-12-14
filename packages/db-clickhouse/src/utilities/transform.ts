import type { TypeWithID } from 'payload'

import type { DataRow, PaginatedResult } from '../types.js'

/**
 * Parse the data field from a ClickHouse row
 * Handles both string JSON and already parsed objects
 */
export function parseDataRow(row: DataRow): DataRow {
  return {
    ...row,
    data: typeof row.data === 'string' ? JSON.parse(row.data) : row.data,
  }
}

/**
 * Transform a ClickHouse row to a Payload document
 */
export function rowToDocument<T extends TypeWithID = TypeWithID>(row: DataRow): T {
  const doc = {
    id: row.id,
    ...row.data,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  } as unknown as T

  return doc
}

/**
 * Transform an array of ClickHouse rows to Payload documents
 */
export function rowsToDocuments<T extends TypeWithID = TypeWithID>(rows: DataRow[]): T[] {
  return rows.map((row) => rowToDocument<T>(row))
}

/**
 * Create a paginated result object
 */
export function createPaginatedResult<T extends TypeWithID = TypeWithID>(
  docs: T[],
  totalDocs: number,
  page: number,
  limit: number,
): PaginatedResult<T> {
  const totalPages = limit > 0 ? Math.ceil(totalDocs / limit) : 1
  const hasNextPage = page < totalPages
  const hasPrevPage = page > 1

  return {
    docs,
    hasNextPage,
    hasPrevPage,
    limit,
    nextPage: hasNextPage ? page + 1 : null,
    page,
    pagingCounter: (page - 1) * limit + 1,
    prevPage: hasPrevPage ? page - 1 : null,
    totalDocs,
    totalPages,
  }
}

/**
 * Extract the title field value from document data
 */
export function extractTitle(
  data: Record<string, unknown>,
  titleField: string | undefined,
  id: string,
): string {
  if (titleField && data[titleField] !== undefined) {
    const value = data[titleField]
    if (typeof value === 'string') {
      return value
    }
    if (value !== null && value !== undefined) {
      return String(value)
    }
  }

  // Fallback to common title fields
  if (typeof data.title === 'string') {return data.title}
  if (typeof data.name === 'string') {return data.name}
  if (typeof data.label === 'string') {return data.label}
  if (typeof data.email === 'string') {return data.email}
  if (typeof data.slug === 'string') {return data.slug}

  return id
}

/**
 * Convert a Payload document to insert data
 */
export function documentToInsertData(
  doc: Record<string, unknown>,
  options: {
    existingCreatedAt?: number
    existingCreatedBy?: null | string
    id: string
    isUpdate?: boolean
    ns: string
    titleField?: string
    type: string
    userId?: string
  },
): {
  createdAt: number
  createdBy: null | string
  data: string
  deletedAt: null | number
  deletedBy: null | string
  id: string
  ns: string
  title: string
  type: string
  updatedAt: number
  updatedBy: null | string
  v: number
} {
  const now = Date.now()
  const { id, createdAt, updatedAt, ...data } = doc

  return {
    id: options.id,
    type: options.type,
    createdAt: options.isUpdate && options.existingCreatedAt ? options.existingCreatedAt : now,
    createdBy: options.isUpdate ? (options.existingCreatedBy ?? null) : (options.userId ?? null),
    data: JSON.stringify(data),
    deletedAt: null,
    deletedBy: null,
    ns: options.ns,
    title: extractTitle(doc, options.titleField, options.id),
    updatedAt: now,
    updatedBy: options.userId ?? null,
    v: now,
  }
}

/**
 * Parse a DateTime64 string from ClickHouse to a Date object
 */
export function parseDateTime64(value: string): Date {
  return new Date(value)
}

/**
 * Parse a DateTime64 string to milliseconds timestamp
 */
export function parseDateTime64ToMs(value: string): number {
  return new Date(value).getTime()
}
