import type { ClickHouseClient } from '@clickhouse/client-web'
import type { DurableObjectState, DurableObjectStorage } from '@cloudflare/workers-types'
import type { BuildQueryJoinAliases, DrizzleAdapter, extendDrizzleTable } from '@payloadcms/drizzle'
import type { BaseSQLiteAdapter, BaseSQLiteArgs } from '@payloadcms/drizzle/sqlite'
import type { DrizzleConfig, Relation, Relations, SQL } from 'drizzle-orm'
import type { DrizzleSqliteDODatabase } from 'drizzle-orm/durable-sqlite'
import type { LibSQLDatabase } from 'drizzle-orm/libsql'
import type {
  AnySQLiteColumn,
  SQLiteInsertOnConflictDoUpdateConfig,
  SQLiteTableWithColumns,
  SQLiteTransactionConfig,
} from 'drizzle-orm/sqlite-core'
import type { SQLiteRaw } from 'drizzle-orm/sqlite-core/query-builders/raw'
import type { Payload, PayloadRequest } from 'payload'

// ClickHouse configuration
export interface ClickHouseConfig {
  /** ClickHouse database name (default: 'default') */
  database?: string
  /** ClickHouse password */
  password?: string
  /** ClickHouse table name (default: 'data') */
  table?: string
  /** ClickHouse server URL (e.g., 'https://host:8443') */
  url: string
  /** ClickHouse username (default: 'default') */
  username?: string
}

// Sync configuration
export interface SyncConfig {
  /** Max operations per sync batch (default: 100) */
  batchSize?: number
  /** Milliseconds to wait before syncing after mutation (default: 100) */
  batchWindow?: number
  /** Days to retain synced oplog entries (default: 7) */
  retentionDays?: number
}

// Index configuration per collection
export type IndexConfig = {
  [collection: string]: string[]
}

// Main adapter args
export interface DOClickHouseAdapterArgs extends Omit<BaseSQLiteArgs, 'url'> {
  /** ClickHouse connection configuration */
  clickhouse: ClickHouseConfig
  /** Durable Object context */
  ctx: DurableObjectState
  /** Optional: fields to index in SQLite per collection */
  fieldIndexes?: IndexConfig
  /**
   * Function to derive DO namespace ID from request
   * Default: () => namespace
   * With multi-tenant: (req) => `${namespace}:${req.headers.get('x-payload-tenant')}`
   */
  getNamespaceId?: (req: PayloadRequest) => string
  /** Namespace for this deployment (e.g., 'prod', 'staging') */
  namespace: string
  /**
   * Experimental. Enables read replicas support with the `first-primary` strategy.
   * @experimental
   */
  readReplicas?: 'first-primary'
  /** Durable Object storage */
  storage: DurableObjectStorage
  /** Sync configuration */
  sync?: SyncConfig
}

// SQLite schema types
type SQLiteSchema = {
  relations: Record<string, GenericRelation>
  tables: Record<string, SQLiteTableWithColumns<any>>
}

type SQLiteSchemaHookArgs = {
  extendTable: typeof extendDrizzleTable
  schema: SQLiteSchema
}

export type SQLiteSchemaHook = (args: SQLiteSchemaHookArgs) => Promise<SQLiteSchema> | SQLiteSchema

export type GenericColumns = {
  [x: string]: AnySQLiteColumn
}

export type GenericTable = SQLiteTableWithColumns<{
  columns: GenericColumns
  dialect: string
  name: string
  schema: string
}>

export type GenericRelation = Relations<string, Record<string, Relation<string>>>

export type CountDistinct = (args: {
  db: LibSQLDatabase
  joins: BuildQueryJoinAliases
  tableName: string
  where: SQL
}) => Promise<number>

export type DeleteWhere = (args: {
  db: LibSQLDatabase
  tableName: string
  where: SQL
}) => Promise<void>

export type { DropDatabase } from '@payloadcms/drizzle/sqlite'

export type Execute<T> = (args: {
  db?: LibSQLDatabase
  drizzle?: LibSQLDatabase
  raw?: string
  sql?: SQL<unknown>
}) => SQLiteRaw<any> | SQLiteRaw<Promise<T>>

export type Insert = (args: {
  db: LibSQLDatabase
  onConflictDoUpdate?: SQLiteInsertOnConflictDoUpdateConfig<any>
  tableName: string
  values: Record<string, unknown> | Record<string, unknown>[]
}) => Promise<Record<string, unknown>[]>

// Explicitly omit drizzle property for complete override
type SQLiteDrizzleAdapter = Omit<
  DrizzleAdapter,
  | 'countDistinct'
  | 'deleteWhere'
  | 'drizzle'
  | 'dropDatabase'
  | 'execute'
  | 'idType'
  | 'insert'
  | 'operators'
  | 'relations'
>

export interface GeneratedDatabaseSchema {
  schemaUntyped: Record<string, unknown>
}

type Drizzle = { $client: DurableObjectStorage } & DrizzleSqliteDODatabase<Record<string, any>>

// Oplog entry for tracking mutations
export interface OplogEntry {
  collection: string
  data: null | Record<string, unknown>
  doc_id: string
  op: 'delete' | 'insert' | 'update'
  seq?: number
  synced: 0 | 1
  timestamp: number
}

// ClickHouse data row
export interface ClickHouseDataRow {
  createdAt: string
  createdBy: null | string
  data: Record<string, unknown>
  deletedAt: null | string
  deletedBy: null | string
  id: string
  ns: string
  tenant: string
  title: string
  type: string
  updatedAt: string
  updatedBy: null | string
  v: string
}

// Main adapter interface
export type DOClickHouseAdapter = {
  // Sync methods
  /** Append operation to oplog (synchronous) */
  appendToOplog: (entry: Omit<OplogEntry, 'seq' | 'synced' | 'timestamp'>) => void
  /** ClickHouse client instance */
  clickhouse: ClickHouseClient | null
  /** ClickHouse configuration */
  clickhouseConfig: ClickHouseConfig
  /** SQLite storage client */
  client: DurableObjectStorage
  /** Durable Object context */
  ctx: DurableObjectState
  /** SQLite Drizzle instance */
  drizzle: Drizzle
  /** Index configuration for collections */
  fieldIndexes: IndexConfig
  /** Function to get namespace ID from request */
  getNamespaceId: (req: PayloadRequest) => string
  /** Namespace for this deployment */
  namespace: string
  /**
   * Experimental. Enables read replicas support.
   */
  readReplicas?: 'first-primary'
  /** Durable Object storage */
  storage: DurableObjectStorage
  /** Force immediate sync to ClickHouse */
  sync: () => Promise<{ synced: number }>
  /** Sync configuration */
  syncConfig: Required<SyncConfig>
} & BaseSQLiteAdapter &
  SQLiteDrizzleAdapter

export type IDType = 'integer' | 'numeric' | 'text'

export type MigrateUpArgs = {
  db: Drizzle
  payload: Payload
  req: PayloadRequest
}

export type MigrateDownArgs = {
  db: Drizzle
  payload: Payload
  req: PayloadRequest
}

declare module 'payload' {
  export interface DatabaseAdapter
    extends Omit<DOClickHouseAdapterArgs, 'idType' | 'logger' | 'migrationDir' | 'pool'>,
      DrizzleAdapter {
    beginTransaction: (options?: SQLiteTransactionConfig) => Promise<null | number | string>
    /** ClickHouse client for cross-DO queries */
    clickhouse: ClickHouseClient | null
    drizzle: Drizzle
    fieldConstraints: Record<string, Record<string, string>>
    idType: DOClickHouseAdapterArgs['idType']
    initializing: Promise<void>
    localesSuffix?: string
    logger: DrizzleConfig['logger']
    /** Current namespace */
    namespace: string
    prodMigrations?: {
      down: (args: MigrateDownArgs) => Promise<void>
      name: string
      up: (args: MigrateUpArgs) => Promise<void>
    }[]
    push: boolean
    rejectInitializing: () => void
    relationshipsSuffix?: string
    resolveInitializing: () => void
    schema: Record<string, GenericRelation | GenericTable>
    /** Force sync to ClickHouse */
    sync: () => Promise<{ synced: number }>
    tableNameMap: Map<string, string>
    transactionOptions: SQLiteTransactionConfig
    versionsSuffix?: string
  }
}
