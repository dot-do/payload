import type { CollectionConfig, Field, FlattenedField, SanitizedConfig, TypeWithID } from 'payload'

import { flattenAllFields } from 'payload'
import { fieldAffectsData, fieldShouldBeLocalized } from 'payload/shared'

import type { DataRow, PaginatedResult } from '../types.js'

/**
 * Sensitive fields that should always be stripped from document data
 */
const SENSITIVE_FIELDS = ['password', 'confirm-password']

/**
 * Strip sensitive fields from document data
 * Similar to MongoDB's stripFields function in transform.ts
 */
export function stripSensitiveFields(data: Record<string, unknown>): Record<string, unknown> {
  const result = { ...data }
  for (const field of SENSITIVE_FIELDS) {
    delete result[field]
  }
  return result
}

/**
 * Strip fields that are not defined in the collection schema
 * Used during read operations to ensure only valid fields are returned
 */
export function stripUndefinedFields(
  data: Record<string, unknown>,
  config: SanitizedConfig,
  fields: Field[],
  reservedKeys: string[] = ['id', 'globalType'],
): Record<string, unknown> {
  const flattenedFields = flattenAllFields({ cache: true, fields })
  const result = { ...data }

  // Remove keys that are not in the field definitions and not reserved
  for (const key in result) {
    if (
      !flattenedFields.some((field) => field.name === key) &&
      !reservedKeys.includes(key) &&
      !SENSITIVE_FIELDS.includes(key)
    ) {
      // Keep the field if it's a valid field
      continue
    }
    if (SENSITIVE_FIELDS.includes(key)) {
      delete result[key]
    }
  }

  // Always strip sensitive fields regardless
  for (const field of SENSITIVE_FIELDS) {
    delete result[field]
  }

  return result
}

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
 * Check if a collection has a custom numeric ID field
 */
export function hasCustomNumericID(fields: Field[]): boolean {
  const idField = fields.find((field) => fieldAffectsData(field) && field.name === 'id')
  return idField?.type === 'number'
}

/**
 * Convert ID to the appropriate type based on collection config
 */
export function convertID(id: string, isNumeric: boolean): number | string {
  if (isNumeric) {
    const num = Number(id)
    return isNaN(num) ? id : num
  }
  return id
}

/**
 * Transform a ClickHouse row to a Payload document
 * @param row The data row from ClickHouse
 * @param numericID If true, convert ID to number (for collections with custom numeric ID fields)
 */
export function rowToDocument<T extends TypeWithID = TypeWithID>(
  row: DataRow,
  numericID = false,
): T {
  // Strip sensitive fields from the data
  const sanitizedData = stripSensitiveFields(row.data)

  const doc = {
    id: numericID ? convertID(row.id, true) : row.id,
    ...sanitizedData,
    createdAt: toISOStringFromClickHouse(row.createdAt),
    updatedAt: toISOStringFromClickHouse(row.updatedAt),
  } as unknown as T

  return doc
}

/**
 * Convert ClickHouse DateTime64 to ISO string, handling UTC correctly
 * ClickHouse returns timestamps without timezone info, but they are UTC.
 */
function toISOStringFromClickHouse(value: null | string | undefined): string | undefined {
  if (!value) {
    return undefined
  }
  // ClickHouse returns DateTime64 like "2021-01-01 00:00:00.000" (space separator, no TZ)
  // We need to convert to proper ISO format with UTC indicator
  const utcValue = value.endsWith('Z') ? value : value.replace(' ', 'T') + 'Z'
  return new Date(utcValue).toISOString()
}

/**
 * Transform an array of ClickHouse rows to Payload documents
 * @param rows The data rows from ClickHouse
 * @param numericID If true, convert IDs to numbers (for collections with custom numeric ID fields)
 */
export function rowsToDocuments<T extends TypeWithID = TypeWithID>(
  rows: DataRow[],
  numericID = false,
): T[] {
  return rows.map((row) => rowToDocument<T>(row, numericID))
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
 * Handles ClickHouse DateTime64 format which doesn't include timezone info
 */
export function parseDateTime64(value: string): Date {
  // ClickHouse returns DateTime64 without timezone, but values are UTC
  const utcValue = value.endsWith('Z') ? value : value.replace(' ', 'T') + 'Z'
  return new Date(utcValue)
}

/**
 * Parse a DateTime64 string to milliseconds timestamp
 * Handles ClickHouse DateTime64 format which doesn't include timezone info
 */
export function parseDateTime64ToMs(value: string): number {
  // ClickHouse returns DateTime64 without timezone, but values are UTC
  const utcValue = value.endsWith('Z') ? value : value.replace(' ', 'T') + 'Z'
  return new Date(utcValue).getTime()
}

/**
 * Convert ClickHouse DateTime format to ISO 8601 format
 * ClickHouse returns: "2021-01-01 00:00:00.000" (without timezone)
 * ISO 8601 expects: "2021-01-01T00:00:00.000Z"
 *
 * Important: ClickHouse DateTime64 values are stored/returned in UTC when session_timezone='UTC',
 * but the string doesn't include timezone info. We must append 'Z' to indicate UTC.
 */
export function toISOString(value: null | string | undefined): string | undefined {
  if (!value) {
    return undefined
  }
  // Append 'Z' to indicate UTC since ClickHouse returns timestamps without timezone
  const utcValue = value.endsWith('Z') ? value : value.replace(' ', 'T') + 'Z'
  return new Date(utcValue).toISOString()
}

/**
 * Check if a value is a plain object (not array, date, null, etc.)
 */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object') {
    return false
  }
  const proto = Object.getPrototypeOf(value)
  return proto === Object.prototype || proto === null
}

/**
 * Check if a value has a $push operator
 */
function has$Push(value: unknown): value is { $push: unknown } {
  return isPlainObject(value) && '$push' in value
}

/**
 * Check if a value has a $remove operator
 */
function has$Remove(value: unknown): value is { $remove: unknown } {
  return isPlainObject(value) && '$remove' in value
}

/**
 * Check if a value has a $inc operator
 */
function has$Inc(value: unknown): value is { $inc: number } {
  return isPlainObject(value) && '$inc' in value && typeof value.$inc === 'number'
}

/**
 * Compare two relationship values for equality
 * Handles both simple IDs and polymorphic relationships { relationTo, value }
 */
function relationshipsEqual(a: unknown, b: unknown): boolean {
  if (a === b) {
    return true
  }

  // Handle polymorphic relationships
  if (
    isPlainObject(a) &&
    isPlainObject(b) &&
    'relationTo' in a &&
    'relationTo' in b &&
    'value' in a &&
    'value' in b
  ) {
    return a.relationTo === b.relationTo && a.value === b.value
  }

  return false
}

/**
 * Apply $push operator to an array
 * Appends items avoiding duplicates
 */
function apply$Push(existing: unknown[], itemsToPush: unknown): unknown[] {
  const items = Array.isArray(itemsToPush) ? itemsToPush : [itemsToPush]
  const result = [...existing]

  for (const item of items) {
    // Check if item already exists (avoid duplicates)
    const exists = result.some((existingItem) => relationshipsEqual(existingItem, item))
    if (!exists) {
      result.push(item)
    }
  }

  return result
}

/**
 * Apply $remove operator to an array
 * Removes matching items
 */
function apply$Remove(existing: unknown[], itemsToRemove: unknown): unknown[] {
  const items = Array.isArray(itemsToRemove) ? itemsToRemove : [itemsToRemove]

  return existing.filter((existingItem) => {
    return !items.some((itemToRemove) => relationshipsEqual(existingItem, itemToRemove))
  })
}

/**
 * Expand dot notation keys into nested objects
 * e.g., { 'a.b.c': value } => { a: { b: { c: value } } }
 */
function expandDotNotation(obj: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {}

  for (const key of Object.keys(obj)) {
    if (key.includes('.')) {
      const parts = key.split('.')
      let current = result
      for (let i = 0; i < parts.length - 1; i++) {
        const part = parts[i]!
        if (!(part in current) || !isPlainObject(current[part])) {
          current[part] = {}
        }
        current = current[part] as Record<string, unknown>
      }
      current[parts[parts.length - 1]!] = obj[key]
    } else {
      result[key] = obj[key]
    }
  }

  return result
}

/**
 * Deep merge two objects
 * Used to properly merge localized data without losing locale keys
 *
 * Handles edge cases:
 * - Circular references (uses seen WeakSet to detect and skip)
 * - Null prototype objects
 * - Arrays are not merged, source overwrites target
 * - $push operator: appends items to existing array, avoiding duplicates
 * - $remove operator: removes items from existing array
 * - Dot notation keys: 'a.b.c' is expanded to nested { a: { b: { c: ... } } }
 */
export function deepMerge<T extends Record<string, unknown>>(
  target: T,
  source: Record<string, unknown>,
  seen: WeakSet<object> = new WeakSet(),
): T {
  // Expand dot notation keys in source
  const expandedSource = expandDotNotation(source)

  // Detect circular references
  if (seen.has(expandedSource)) {
    return target
  }
  seen.add(expandedSource)

  const result = { ...target }

  for (const key of Object.keys(expandedSource)) {
    const sourceValue = expandedSource[key]
    const targetValue = result[key]

    // Handle $push operator
    if (has$Push(sourceValue)) {
      const existingArray = Array.isArray(targetValue) ? targetValue : []
      result[key as keyof T] = apply$Push(existingArray, sourceValue.$push) as T[keyof T]
      continue
    }

    // Handle $remove operator
    if (has$Remove(sourceValue)) {
      const existingArray = Array.isArray(targetValue) ? targetValue : []
      result[key as keyof T] = apply$Remove(existingArray, sourceValue.$remove) as T[keyof T]
      continue
    }

    // Handle $inc operator
    if (has$Inc(sourceValue)) {
      const existingValue = typeof targetValue === 'number' ? targetValue : 0
      result[key as keyof T] = (existingValue + sourceValue.$inc) as T[keyof T]
      continue
    }

    // If both values are plain objects (not arrays, dates, etc.), recursively merge
    if (isPlainObject(sourceValue) && isPlainObject(targetValue)) {
      // Check for circular reference in source value
      if (seen.has(sourceValue)) {
        continue
      }
      result[key as keyof T] = deepMerge(targetValue, sourceValue, seen) as T[keyof T]
    } else {
      // Otherwise, overwrite with source value
      result[key as keyof T] = sourceValue as T[keyof T]
    }
  }

  return result
}
