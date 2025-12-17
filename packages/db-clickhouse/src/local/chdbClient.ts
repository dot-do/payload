/**
 * A wrapper around chdb Session that provides a @clickhouse/client-web compatible interface
 * This allows the local adapter to use the same operations as the remote adapter
 */

import type { ChdbSession } from '../types.js'

/**
 * Parameters for command execution (INSERT, DELETE, CREATE, etc.)
 */
export interface CommandParams {
  query: string
  query_params?: Record<string, unknown>
}

/**
 * Parameters for query execution (SELECT)
 */
export interface QueryParams {
  format?: string
  query: string
  query_params?: Record<string, unknown>
}

/**
 * Result from a query execution
 */
export interface QueryResult<T = unknown> {
  json: <R = T>() => Promise<R[]>
  text: () => Promise<string>
}

/**
 * Convert ClickHouse parameter format {name:Type} to chdb format
 * ClickHouse uses {name:Type} syntax, but chdb queryBind expects simple {key: value}
 * We extract parameter names and substitute values
 */
function substituteParams(query: string, params?: Record<string, unknown>): string {
  if (!params) {
    return query
  }

  // Replace ClickHouse-style parameters {name:Type} with actual values
  return query.replace(/\{(\w+):[^}]+\}/g, (match, paramName) => {
    if (!(paramName in params)) {
      throw new Error(`Missing parameter: ${paramName}`)
    }

    const value = params[paramName]

    // Handle different types
    if (value === null || value === undefined) {
      return 'NULL'
    }
    if (typeof value === 'string') {
      // Escape single quotes in strings
      return `'${value.replace(/'/g, "''")}'`
    }
    if (typeof value === 'number' || typeof value === 'bigint') {
      return String(value)
    }
    if (typeof value === 'boolean') {
      return value ? '1' : '0'
    }
    if (Array.isArray(value)) {
      const escaped = value.map((v) =>
        typeof v === 'string' ? `'${v.replace(/'/g, "''")}'` : String(v),
      )
      return `[${escaped.join(', ')}]`
    }

    // For objects, convert to JSON string
    return `'${JSON.stringify(value).replace(/'/g, "''")}'`
  })
}

/**
 * ChDB client wrapper providing @clickhouse/client-web compatible interface
 */
export class ChdbClient {
  private session: ChdbSession

  constructor(session: ChdbSession) {
    this.session = session
  }

  /**
   * Close the client and cleanup resources
   */
  close(): Promise<void> {
    this.session.cleanup()
    return Promise.resolve()
  }

  /**
   * Execute a command (INSERT, DELETE, CREATE, etc.) that doesn't return data
   */
  command(params: CommandParams): Promise<void> {
    const query = substituteParams(params.query, params.query_params)
    this.session.query(query)
    return Promise.resolve()
  }

  /**
   * Insert data into a table
   */
  insert(params: { format?: string; table: string; values: unknown[] }): Promise<void> {
    const { format = 'JSONEachRow', table, values } = params

    if (!values.length) {
      return Promise.resolve()
    }

    const data = values.map((v) => JSON.stringify(v)).join('\n')
    const query = `INSERT INTO ${table} FORMAT ${format}\n${data}`
    this.session.query(query)
    return Promise.resolve()
  }

  /**
   * Execute a query (SELECT) and return results
   */
  query<T = unknown>(params: QueryParams): Promise<QueryResult<T>> {
    const query = substituteParams(params.query, params.query_params)
    const format = params.format || 'JSONEachRow'
    const result = this.session.query(query, format)

    return Promise.resolve({
      json: <R = T>(): Promise<R[]> => {
        if (!result || result.trim() === '') {
          return Promise.resolve([])
        }

        // JSONEachRow format: each line is a JSON object
        if (format === 'JSONEachRow') {
          return Promise.resolve(
            result
              .trim()
              .split('\n')
              .filter((line) => line.trim())
              .map((line) => JSON.parse(line) as R),
          )
        }

        // JSON format: single JSON array
        if (format === 'JSON') {
          const parsed = JSON.parse(result)
          return Promise.resolve((parsed.data || parsed) as R[])
        }

        // For other formats, try parsing as JSON array
        try {
          return Promise.resolve(JSON.parse(result) as R[])
        } catch {
          return Promise.resolve([])
        }
      },
      text: (): Promise<string> => Promise.resolve(result),
    })
  }
}
