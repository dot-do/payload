/**
 * Better-SQLite3 based DurableObjectStorage for testing
 *
 * This creates a DurableObjectStorage-compatible interface using better-sqlite3,
 * providing synchronous SQL operations that match the Cloudflare Workers runtime.
 *
 * The drizzle-orm durable-sqlite driver expects synchronous sql.exec() calls,
 * so we use better-sqlite3 (which is synchronous) instead of async miniflare.
 */
import Database from 'better-sqlite3'

/**
 * Cursor that wraps results from better-sqlite3 SQL operations
 * Matches the SqlStorageCursor interface expected by drizzle-orm
 */
class BetterSqliteCursor<T extends Record<string, unknown>> {
  private _rowsRead: number
  private _rowsWritten: number
  private index = 0
  private results: T[]
  public columnNames: string[]

  constructor(results: T[], columnNames: string[], rowsRead: number, rowsWritten: number) {
    this.results = results
    this.columnNames = columnNames
    this._rowsRead = rowsRead
    this._rowsWritten = rowsWritten
  }

  next(): { done: true; value?: never } | { done?: false; value: T } {
    if (this.index < this.results.length) {
      return { done: false, value: this.results[this.index++] }
    }
    return { done: true }
  }

  one(): T {
    return this.results[0]
  }

  raw<U extends unknown[]>(): { toArray: () => U[] } & IterableIterator<U> {
    const rows = this.results.map((row) => Object.values(row) as U)
    let index = 0
    const iterator: { toArray: () => U[] } & IterableIterator<U> = {
      next: () => {
        if (index < rows.length) {
          return { done: false, value: rows[index++] }
        }
        return { done: true, value: undefined as unknown as U }
      },
      [Symbol.iterator]() {
        return this
      },
      toArray: () => rows,
    }
    return iterator
  }

  [Symbol.iterator](): IterableIterator<T> {
    return this.results[Symbol.iterator]()
  }

  toArray(): T[] {
    return this.results
  }

  get rowsRead(): number {
    return this._rowsRead
  }

  get rowsWritten(): number {
    return this._rowsWritten
  }
}

/**
 * SQL storage interface that wraps better-sqlite3
 * Matches the DurableObjectStorage.sql interface
 */
class BetterSqliteStorage {
  private db: Database.Database

  constructor(db: Database.Database) {
    this.db = db
  }

  exec<T extends Record<string, unknown>>(
    query: string,
    ...bindings: unknown[]
  ): BetterSqliteCursor<T> {
    // Try to execute the query, handling ambiguous column names
    let currentQuery = query
    let attempts = 0
    const maxAttempts = 10

    while (attempts < maxAttempts) {
      try {
        const trimmedQuery = currentQuery.trim().toLowerCase()
        const isSelect = trimmedQuery.startsWith('select')
        const isReturning = trimmedQuery.includes(' returning ')
        // PRAGMA statements that query info return data, setter statements don't
        const isPragmaQuery = trimmedQuery.startsWith('pragma') && !trimmedQuery.includes('=')

        let results: T[] = []
        let columnNames: string[] = []
        let rowsRead = 0
        let rowsWritten = 0

        if (isSelect || isPragmaQuery || isReturning) {
          // For SELECT/PRAGMA queries/RETURNING queries, use all() to get rows
          const stmt = this.db.prepare(currentQuery)
          results = (bindings.length > 0 ? stmt.all(...bindings) : stmt.all()) as T[]
          rowsRead = results.length

          // Get column names from the statement
          const columns = stmt.columns()
          columnNames = columns.map((col) => col.name)
        } else {
          // For INSERT/UPDATE/DELETE/PRAGMA setters, use run()
          const stmt = this.db.prepare(currentQuery)
          const result = bindings.length > 0 ? stmt.run(...bindings) : stmt.run()
          rowsWritten = result.changes
          rowsRead = 0
        }

        return new BetterSqliteCursor<T>(results, columnNames, rowsRead, rowsWritten)
      } catch (err) {
        const error = err as Error
        // Handle "ambiguous column name" error by qualifying the column
        if (error.message.includes('ambiguous column name:')) {
          const match = error.message.match(/ambiguous column name: (\w+)/)
          if (match) {
            const ambiguousColumn = match[1]
            // Find the first table in FROM clause to qualify the column
            const fromMatch = currentQuery.match(/from\s+["']?(\w+)["']?/i)
            if (fromMatch) {
              const tableName = fromMatch[1]
              // Replace unqualified column references with qualified ones
              // Look for the column name not preceded by a dot or table name
              const columnRegex = new RegExp(`(?<![."'\\w])\\b${ambiguousColumn}\\b(?![."'])`, 'gi')
              const newQuery = currentQuery.replace(
                columnRegex,
                `"${tableName}"."${ambiguousColumn}"`,
              )
              if (newQuery !== currentQuery) {
                currentQuery = newQuery
                attempts++
                continue
              }
            }
          }
        }
        // Rethrow with better context if not an ambiguous column error or couldn't fix it
        throw new Error(`SQL error: ${error.message}\nQuery: ${query}`)
      }
    }
    throw new Error(`Failed to resolve ambiguous column names after ${maxAttempts} attempts`)
  }

  get databaseSize(): number {
    const result = this.db.pragma('page_count') as { page_count: number }[]
    const pageSize = (this.db.pragma('page_size') as { page_size: number }[])[0]?.page_size || 4096
    return (result[0]?.page_count || 0) * pageSize
  }
}

/**
 * DurableObjectStorage implementation using better-sqlite3
 * Provides both SQL and KV operations matching the DO storage interface
 */
export class BetterSqliteProxyStorage {
  private alarm: null | number = null
  private db: Database.Database
  private kvData: Map<string, unknown> = new Map()
  public sql: BetterSqliteStorage

  constructor(db: Database.Database) {
    this.db = db
    this.sql = new BetterSqliteStorage(db)
  }

  delete(key: string): Promise<boolean>
  delete(keys: string[]): Promise<number>
  delete(keyOrKeys: string | string[]): Promise<boolean | number> {
    if (Array.isArray(keyOrKeys)) {
      let count = 0
      for (const key of keyOrKeys) {
        if (this.kvData.delete(key)) {
          count++
        }
      }
      return Promise.resolve(count)
    }
    return Promise.resolve(this.kvData.delete(keyOrKeys))
  }

  deleteAlarm(): Promise<void> {
    this.alarm = null
    return Promise.resolve()
  }
  deleteAll(): Promise<void> {
    this.kvData.clear()
    return Promise.resolve()
  }
  // KV operations
  get<T = unknown>(key: string): Promise<T | undefined>

  get<T = unknown>(keys: string[]): Promise<Map<string, T>>
  get<T = unknown>(keyOrKeys: string | string[]): Promise<Map<string, T> | T | undefined> {
    if (Array.isArray(keyOrKeys)) {
      const result = new Map<string, T>()
      for (const key of keyOrKeys) {
        const value = this.kvData.get(key)
        if (value !== undefined) {
          result.set(key, value as T)
        }
      }
      return Promise.resolve(result)
    }
    return Promise.resolve(this.kvData.get(keyOrKeys) as T | undefined)
  }
  // Alarm operations
  getAlarm(): Promise<null | number> {
    return Promise.resolve(this.alarm)
  }

  getBookmarkForTime(): Promise<string> {
    return Promise.resolve('')
  }

  getCurrentBookmark(): Promise<string> {
    return Promise.resolve('')
  }

  list<T = unknown>(): Promise<Map<string, T>> {
    return Promise.resolve(new Map(this.kvData) as Map<string, T>)
  }

  onNextSessionRestoreBookmark(): Promise<string> {
    return Promise.resolve('')
  }

  put<T>(key: string, value: T): Promise<void>

  put<T>(entries: Record<string, T>): Promise<void>

  put<T>(keyOrEntries: Record<string, T> | string, value?: T): Promise<void> {
    if (typeof keyOrEntries === 'string') {
      this.kvData.set(keyOrEntries, value)
    } else {
      for (const [k, v] of Object.entries(keyOrEntries)) {
        this.kvData.set(k, v)
      }
    }
    return Promise.resolve()
  }

  setAlarm(time: number): Promise<void> {
    this.alarm = time
    return Promise.resolve()
  }

  // Sync operations (no-op for testing)
  sync(): Promise<void> {
    return Promise.resolve()
  }

  // Transaction support
  async transaction<T>(closure: (txn: BetterSqliteProxyStorage) => Promise<T>): Promise<T> {
    return closure(this)
  }

  transactionSync<T>(closure: () => T): T {
    return this.db.transaction(closure)()
  }
}

/**
 * DurableObjectState implementation for testing
 */
export class BetterSqliteProxyState {
  public id: { toString: () => string }
  public storage: BetterSqliteProxyStorage

  constructor(storage: BetterSqliteProxyStorage, doId: string) {
    this.storage = storage
    this.id = { toString: () => doId }
  }

  async blockConcurrencyWhile<T>(closure: () => Promise<T>): Promise<T> {
    return closure()
  }

  waitUntil(): void {}
}

// Keep track of databases for cleanup
const databases: Map<string, Database.Database> = new Map()

/**
 * Create a DurableObjectStorage backed by better-sqlite3
 * This provides the same synchronous SQL interface as Cloudflare Durable Objects
 */
export function createMockDOStorage(doId: string = 'test'): Promise<{
  ctx: BetterSqliteProxyState
  dispose: () => Promise<void>
  storage: BetterSqliteProxyStorage
}> {
  // Create an in-memory SQLite database
  const db = new Database(':memory:')

  // Enable foreign keys
  db.pragma('foreign_keys = ON')

  // Store for cleanup
  databases.set(doId, db)

  const storage = new BetterSqliteProxyStorage(db)
  const ctx = new BetterSqliteProxyState(storage, doId)

  const dispose = (): Promise<void> => {
    db.close()
    databases.delete(doId)
    return Promise.resolve()
  }

  return Promise.resolve({ storage, ctx, dispose })
}

// Cleanup function for test teardown
export function disposeMiniflare(): Promise<void> {
  for (const db of databases.values()) {
    db.close()
  }
  databases.clear()
  return Promise.resolve()
}
