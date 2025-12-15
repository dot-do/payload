import type { FindGlobal, FindGlobalArgs, Where } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { assertValidSlug } from '../utilities/sanitize.js'
import { parseDataRow, rowToDocument } from '../utilities/transform.js'

const GLOBALS_TYPE = '_globals'

/**
 * Check if a document matches a where clause (for access control)
 * This is a simple implementation for common access control patterns
 */
function matchesWhere(data: Record<string, unknown>, where: Where): boolean {
  if (!where || Object.keys(where).length === 0) {
    return true
  }

  for (const [key, condition] of Object.entries(where)) {
    // Handle AND conditions
    if (key === 'and' && Array.isArray(condition)) {
      if (!condition.every((c) => matchesWhere(data, c))) {
        return false
      }
      continue
    }

    // Handle OR conditions
    if (key === 'or' && Array.isArray(condition)) {
      if (!condition.some((c) => matchesWhere(data, c))) {
        return false
      }
      continue
    }

    // Handle field conditions
    const fieldValue = data[key]
    if (typeof condition === 'object' && condition !== null) {
      const conditionObj = condition as Record<string, unknown>

      if ('equals' in conditionObj) {
        if (fieldValue !== conditionObj.equals) {
          return false
        }
      }
      if ('not_equals' in conditionObj) {
        if (fieldValue === conditionObj.not_equals) {
          return false
        }
      }
      if ('exists' in conditionObj) {
        const exists = fieldValue !== undefined && fieldValue !== null
        if (conditionObj.exists !== exists) {
          return false
        }
      }
    }
  }

  return true
}

export const findGlobal: FindGlobal = async function findGlobal<
  T extends Record<string, unknown> = Record<string, unknown>,
>(this: ClickHouseAdapter, args: FindGlobalArgs): Promise<T> {
  const { slug, where } = args

  assertValidSlug(slug, 'global')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const params: QueryParams = {
    id: slug,
    type: GLOBALS_TYPE,
    ns: this.namespace,
  }

  // Use window function, filter deletedAt after
  const query = `
    SELECT * EXCEPT(_rn)
    FROM (
      SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ns = {ns:String}
        AND type = {type:String}
        AND id = {id:String}
    )
    WHERE _rn = 1 AND deletedAt IS NULL
    LIMIT 1
  `

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = await result.json<DataRow>()

  if (rows.length === 0) {
    // No document exists - return empty object if there's a where clause (access control)
    // Otherwise return base object
    if (where && Object.keys(where).length > 0) {
      return {} as T
    }
    return { id: slug, globalType: slug } as unknown as T
  }

  const parsedRow = parseDataRow(rows[0]!)
  const doc = rowToDocument<{ id: string } & T>(parsedRow)
  const fullDoc = { ...doc, globalType: slug }

  // If there's a where clause (access control), check if document matches
  if (where && Object.keys(where).length > 0) {
    const docData = parsedRow.data
    if (!matchesWhere(docData, where)) {
      return {} as T
    }
  }

  return fullDoc as unknown as T
}
