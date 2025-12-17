/**
 * chdb adapter - Embedded ClickHouse for local development
 *
 * This adapter uses chdb to run ClickHouse embedded in your Node.js process
 * with file-based persistence. No Docker or external server required.
 *
 * @example
 * ```typescript
 * import { chdbAdapter } from '@dotdo/db-clickhouse/local'
 *
 * export default buildConfig({
 *   db: chdbAdapter({
 *     path: './.data/clickhouse', // Directory for data storage
 *     namespace: 'development'
 *   }),
 *   // ...
 * })
 * ```
 */

import type { DatabaseAdapterObj, Payload } from 'payload'

import path from 'path'
import { createDatabaseAdapter } from 'payload'

import type { ClickHouseAdapter, VectorIndexConfig } from './types.js'

import { createMigration } from './createMigration.js'
import { init } from './init.js'
import { ChdbClient } from './local/chdbClient.js'
import { migrate } from './migrate.js'
import { migrateFresh } from './migrateFresh.js'
import {
  beginTransaction,
  commitTransaction,
  count,
  countGlobalVersions,
  countVersions,
  create,
  createGlobal,
  createGlobalVersion,
  createVersion,
  deleteMany,
  deleteOne,
  deleteVersions,
  execute,
  find,
  findDistinct,
  findGlobal,
  findGlobalVersions,
  findOne,
  findVersions,
  getSearchQueue,
  logEvent,
  queryDrafts,
  queryEvents,
  rollbackTransaction,
  search,
  syncToSearch,
  updateGlobal,
  updateGlobalVersion,
  updateMany,
  updateOne,
  updateSearchStatus,
  updateVersion,
  upsert,
  upsertMany,
} from './operations/index.js'
import { assertValidNamespace } from './utilities/sanitize.js'

export interface ChdbAdapterArgs {
  /** Default transaction timeout in ms (default: 30000, null for no timeout) */
  defaultTransactionTimeout?: null | number
  /** Embedding dimensions for vector search (default: 1536) */
  embeddingDimensions?: number
  /** ID type for documents (default: 'text' - nanoid) */
  idType?: 'text' | 'uuid'
  /** Namespace to separate different Payload apps (default: 'payload') */
  namespace?: string
  /** Path to the directory for chdb data storage (default: './.data/clickhouse') */
  path?: string
  /** Table name (default: 'data') */
  table?: string
  /** Vector index configuration for similarity search (default: disabled) */
  vectorIndex?: VectorIndexConfig
}

/**
 * Validate that a table name is safe to use in SQL
 */
function validateTableName(tableName: string): void {
  if (!/^[a-z_]\w*$/i.test(tableName)) {
    throw new Error(
      `Invalid table name '${tableName}'. Table names must start with a letter or underscore and contain only alphanumeric characters and underscores.`,
    )
  }
}

/**
 * Generate SQL to create the data table if it doesn't exist
 */
function getCreateTableSQL(tableName: string): string {
  validateTableName(tableName)
  return `
CREATE TABLE IF NOT EXISTS ${tableName} (
    ns String,
    type String,
    id String,
    v DateTime64(3),
    title String DEFAULT '',
    data JSON,
    createdAt DateTime64(3),
    createdBy Nullable(String),
    updatedAt DateTime64(3),
    updatedBy Nullable(String),
    deletedAt Nullable(DateTime64(3)),
    deletedBy Nullable(String)
) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, type, id, v)
`
}

/**
 * Generate SQL to create the relationships table if it doesn't exist
 */
function getCreateRelationshipsTableSQL(tableName: string): string {
  validateTableName(tableName)
  return `
CREATE TABLE IF NOT EXISTS ${tableName}_relationships (
    ns String,
    fromType String,
    fromId String,
    fromField String,
    toType String,
    toId String,
    position UInt16 DEFAULT 0,
    locale Nullable(String),
    v DateTime64(3, 'UTC'),
    deletedAt Nullable(DateTime64(3, 'UTC'))
) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, toType, toId, fromType, fromId, fromField, position)
`
}

/**
 * Generate SQL to create the actions table if it doesn't exist
 */
function getCreateActionsTableSQL(): string {
  return `
CREATE TABLE IF NOT EXISTS actions (
    txId String,
    txStatus Enum8('pending' = 0, 'committed' = 1, 'aborted' = 2),
    txTimeout Nullable(DateTime64(3)),
    txCreatedAt DateTime64(3),
    id String,
    ns String,
    type String,
    v DateTime64(3),
    data JSON,
    title String DEFAULT '',
    createdAt DateTime64(3),
    createdBy Nullable(String),
    updatedAt DateTime64(3),
    updatedBy Nullable(String),
    deletedAt Nullable(DateTime64(3)),
    deletedBy Nullable(String)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(txCreatedAt)
ORDER BY (ns, txId, type, id)
`
}

/**
 * Generate SQL to create the search table if it doesn't exist
 */
function getCreateSearchTableSQL(
  embeddingDimensions: number,
  vectorIndex?: VectorIndexConfig,
): string {
  // Build vector index clause if enabled
  // ClickHouse vector_similarity requires: INDEX name column TYPE vector_similarity('metric', dimensions)
  const vectorIndexClause = vectorIndex?.enabled
    ? `,
    INDEX embedding_idx embedding TYPE vector_similarity('${vectorIndex.metric || 'L2Distance'}', ${embeddingDimensions}) GRANULARITY 1`
    : ''

  return `
CREATE TABLE IF NOT EXISTS search (
    id String,
    ns String,
    collection String,
    docId String,
    chunkIndex UInt16 DEFAULT 0,
    text String,
    embedding Array(Float32),
    status Enum8('pending' = 0, 'ready' = 1, 'failed' = 2),
    errorMessage Nullable(String),
    createdAt DateTime64(3),
    updatedAt DateTime64(3),
    INDEX text_idx text TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4${vectorIndexClause}
) ENGINE = ReplacingMergeTree(updatedAt)
PARTITION BY ns
ORDER BY (ns, collection, docId, chunkIndex)
`
}

/**
 * Generate SQL to create the events table if it doesn't exist
 */
function getCreateEventsTableSQL(): string {
  return `
CREATE TABLE IF NOT EXISTS events (
    id String,
    ns String,
    timestamp DateTime64(3),
    type String,
    collection Nullable(String),
    docId Nullable(String),
    userId Nullable(String),
    sessionId Nullable(String),
    ip Nullable(String),
    duration UInt32 DEFAULT 0,
    input JSON,
    result JSON
) ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (ns, timestamp, type)
`
}

/**
 * Create an embedded ClickHouse database adapter using chdb
 *
 * This adapter runs ClickHouse embedded in your Node.js process with
 * file-based persistence. Ideal for local development and testing.
 * For production, use the standard clickhouseAdapter with a remote server.
 */
export function chdbAdapter(args: ChdbAdapterArgs = {}): DatabaseAdapterObj {
  const {
    defaultTransactionTimeout = 30_000,
    embeddingDimensions = 1536,
    idType = 'text',
    namespace = 'payload',
    path: dataPath = './.data/clickhouse',
    table = 'data',
    vectorIndex,
  } = args

  // Validate namespace (allows dots for domain names)
  assertValidNamespace(namespace)

  function adapter({ payload }: { payload: Payload }) {
    return createDatabaseAdapter<ClickHouseAdapter>({
      name: 'clickhouse',
      defaultIDType: 'text',
      packageName: '@dotdo/db-clickhouse/local',

      // ClickHouse-specific config (chdb uses embedded database, no URL/auth needed)
      clickhouse: null, // Set during connect
      config: {
        database: 'default',
        defaultTransactionTimeout,
        embeddingDimensions,
        idType,
        namespace,
        password: '',
        table,
        url: `file://${dataPath}`, // Use file:// URL for local storage path
        username: 'default',
        vectorIndex,
      },
      database: 'default',
      defaultTransactionTimeout,
      embeddingDimensions,
      idType,
      namespace,
      table,
      vectorIndex,

      // Core adapter
      payload,

      // Connection - custom for local chdb
      connect: async function connect(this: ClickHouseAdapter): Promise<void> {
        const absolutePath = path.resolve(process.cwd(), dataPath)

        // Create the chdb client
        const client = createChdbClient(absolutePath)
        this.clickhouse = client as unknown as ClickHouseAdapter['clickhouse']

        // Validate and freeze the table name
        validateTableName(this.table)
        Object.defineProperty(this, 'table', {
          configurable: false,
          value: this.table,
          writable: false,
        })

        // Create tables if they don't exist
        await client.command({ query: getCreateTableSQL(this.table) })
        await client.command({ query: getCreateRelationshipsTableSQL(this.table) })
        await client.command({ query: getCreateActionsTableSQL() })
        await client.command({
          query: getCreateSearchTableSQL(this.embeddingDimensions, this.vectorIndex),
        })
        await client.command({ query: getCreateEventsTableSQL() })

        // Delete data for current namespace if PAYLOAD_DROP_DATABASE is set (for tests)
        if (process.env.PAYLOAD_DROP_DATABASE === 'true') {
          this.payload.logger.info(`---- DROPPING DATA FOR NAMESPACE ${this.namespace} ----`)
          await client.command({
            query: `DELETE FROM ${this.table} WHERE ns = '${this.namespace.replace(/'/g, "''")}'`,
          })
          await client.command({
            query: `DELETE FROM ${this.table}_relationships WHERE ns = '${this.namespace.replace(/'/g, "''")}'`,
          })
          await client.command({
            query: `DELETE FROM actions WHERE ns = '${this.namespace.replace(/'/g, "''")}'`,
          })
          await client.command({
            query: `DELETE FROM search WHERE ns = '${this.namespace.replace(/'/g, "''")}'`,
          })
          await client.command({
            query: `DELETE FROM events WHERE ns = '${this.namespace.replace(/'/g, "''")}'`,
          })
          this.payload.logger.info('---- DROPPED NAMESPACE DATA ----')
        }
      },

      destroy: async function destroy(this: ClickHouseAdapter): Promise<void> {
        if (this.clickhouse) {
          await (this.clickhouse as unknown as { close: () => Promise<void> }).close()
          this.clickhouse = null
        }
      },

      init,

      // CRUD operations
      count,
      create,
      deleteMany,
      deleteOne,
      find,
      findDistinct,
      findOne,
      updateMany,
      updateOne,

      // Global operations
      createGlobal,
      findGlobal,
      updateGlobal,

      // Version operations
      countVersions,
      createVersion,
      deleteVersions,
      findVersions,
      updateVersion,

      // Global version operations
      countGlobalVersions,
      createGlobalVersion,
      findGlobalVersions,
      updateGlobalVersion,

      // Draft queries
      queryDrafts,

      // Upsert operations
      upsert,
      upsertMany,

      // Search operations
      getSearchQueue,
      search,
      syncToSearch,
      updateSearchStatus,

      // Event logging
      logEvent,
      queryEvents,

      // Raw query execution
      execute,

      // Transaction operations
      beginTransaction,
      commitTransaction,
      rollbackTransaction,

      // Migration support
      createMigration,
      migrate,
      migrateFresh,
      async migrateRefresh(this: ClickHouseAdapter) {
        if (this.clickhouse) {
          const client = this.clickhouse as unknown as {
            command: (params: { query: string }) => Promise<void>
          }
          await client.command({
            query: `DELETE FROM ${this.table} WHERE ns = '${this.namespace.replace(/'/g, "''")}'`,
          })
        }
        await migrate.call(this)
      },
      async migrateReset(this: ClickHouseAdapter) {
        if (this.clickhouse) {
          const client = this.clickhouse as unknown as {
            command: (params: { query: string }) => Promise<void>
          }
          await client.command({
            query: `DELETE FROM ${this.table} WHERE ns = '${this.namespace.replace(/'/g, "''")}'`,
          })
        }
      },
      migrateStatus: async () => {},
      migrationDir: path.resolve(process.cwd(), 'src/migrations'),
    })
  }

  return {
    name: 'clickhouse',
    defaultIDType: 'text',
    init: adapter,
  }
}

// Re-export ChdbClient for users who want to create their own client
export { ChdbClient } from './local/chdbClient.js'

export type { ChdbSession, VectorIndexConfig } from './types.js'

/**
 * Create a ChDB client with a session at the specified path
 */
export function createChdbClient(path: string): InstanceType<typeof ChdbClient> {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { Session } = require('chdb') as {
    Session: new (path: string) => {
      cleanup(): void
      query(query: string, format?: string): string
    }
  }
  const session = new Session(path)
  return new ChdbClient(session)
}

// Backwards compatibility alias
export { chdbAdapter as localClickhouseAdapter }
