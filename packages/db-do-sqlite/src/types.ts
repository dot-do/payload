import type { ResultSet } from '@libsql/client'
import type { BuildQueryJoinAliases, DrizzleAdapter, extendDrizzleTable } from '@payloadcms/drizzle'
import type { BaseSQLiteAdapter, BaseSQLiteArgs } from '@payloadcms/drizzle/sqlite'
import type { DrizzleConfig, Relation, Relations, SQL } from 'drizzle-orm'
import type { LibSQLDatabase } from 'drizzle-orm/libsql'
import type {
  AnySQLiteColumn,
  SQLiteInsertOnConflictDoUpdateConfig,
  SQLiteTableWithColumns,
  SQLiteTransactionConfig,
} from 'drizzle-orm/sqlite-core'
import type { SQLiteRaw } from 'drizzle-orm/sqlite-core/query-builders/raw'
import type { Payload, PayloadRequest } from 'payload'
import type { DurableObjectState, DurableObjectStorage } from '@cloudflare/workers-types'

type SQLiteSchema = {
  relations: Record<string, GenericRelation>
  tables: Record<string, SQLiteTableWithColumns<any>>
}

type SQLiteSchemaHookArgs = {
  extendTable: typeof extendDrizzleTable
  schema: SQLiteSchema
}

export type SQLiteSchemaHook = (args: SQLiteSchemaHookArgs) => Promise<SQLiteSchema> | SQLiteSchema

export type Args = {
  ctx: DurableObjectState
  storage: DurableObjectStorage
  /**
   * Experimental. Enables read replicas support with the `first-primary` strategy.
   *
   * @experimental
   * @example
   *
   * ```readReplicas: 'first-primary'```
   */
  readReplicas?: 'first-primary'
} & BaseSQLiteArgs

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

export type DropDatabase = (args: { adapter: SQLiteDOAdapter }) => Promise<void>

export type Execute<T> = (args: {
  db?: LibSQLDatabase
  drizzle?: LibSQLDatabase
  raw?: string
  sql?: SQL<unknown>
}) => SQLiteRaw<Promise<T>> | SQLiteRaw<ResultSet>

export type Insert = (args: {
  db: LibSQLDatabase
  onConflictDoUpdate?: SQLiteInsertOnConflictDoUpdateConfig<any>
  tableName: string
  values: Record<string, unknown> | Record<string, unknown>[]
}) => Promise<Record<string, unknown>[]>

// Explicitly omit drizzle property for complete override in SQLiteAdapter, required in ts 5.5
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

type ResolveSchemaType<T> = 'schema' extends keyof T
  ? T['schema']
  : GeneratedDatabaseSchema['schemaUntyped']

// Use the proper Durable SQLite database type from drizzle-orm
// Note: We assume drizzle-orm exports DrizzleDurableDatabase from 'drizzle-orm/durable-sqlite'
// If not available, fallback to LibSQLDatabase which has similar interface
type Drizzle = { $client: DurableObjectStorage } & LibSQLDatabase<Record<string, any>>

export type SQLiteDOAdapter = {
  client: DurableObjectStorage
  ctx: Args['ctx']
  drizzle: Drizzle
  /**
   * Experimental. Enables read replicas support with the `first-primary` strategy.
   *
   * @example
   *
   * ```readReplicas: 'first-primary'```
   */
  readReplicas?: 'first-primary'
  storage: Args['storage']
} & BaseSQLiteAdapter &
  SQLiteDrizzleAdapter

export type IDType = 'integer' | 'numeric' | 'text'

export type MigrateUpArgs = {
  /**
   * The SQLite Drizzle instance that you can use to execute SQL directly within the current transaction.
   * @example
   * ```ts
   * import { type MigrateUpArgs, sql } from '@payloadcms/db-do-sqlite'
   *
   * export async function up({ db, payload, req }: MigrateUpArgs): Promise<void> {
   *   const { rows: posts } = await db.run(sql`SELECT * FROM posts`)
   * }
   * ```
   */
  db: Drizzle
  /**
   * The Payload instance that you can use to execute Local API methods
   * To use the current transaction you must pass `req` to arguments
   * @example
   * ```ts
   * import { type MigrateUpArgs } from '@payloadcms/db-do-sqlite'
   *
   * export async function up({ db, payload, req }: MigrateUpArgs): Promise<void> {
   *   const posts = await payload.find({ collection: 'posts', req })
   * }
   * ```
   */
  payload: Payload
  /**
   * The `PayloadRequest` object that contains the current transaction
   */
  req: PayloadRequest
}
export type MigrateDownArgs = {
  /**
   * The SQLite Drizzle instance that you can use to execute SQL directly within the current transaction.
   * @example
   * ```ts
   * import { type MigrateDownArgs, sql } from '@payloadcms/db-do-sqlite'
   *
   * export async function down({ db, payload, req }: MigrateDownArgs): Promise<void> {
   *   const { rows: posts } = await db.run(sql`SELECT * FROM posts`)
   * }
   * ```
   */
  db: Drizzle
  /**
   * The Payload instance that you can use to execute Local API methods
   * To use the current transaction you must pass `req` to arguments
   * @example
   * ```ts
   * import { type MigrateDownArgs } from '@payloadcms/db-do-sqlite'
   *
   * export async function down({ db, payload, req }: MigrateDownArgs): Promise<void> {
   *   const posts = await payload.find({ collection: 'posts', req })
   * }
   * ```
   */
  payload: Payload
  /**
   * The `PayloadRequest` object that contains the current transaction
   */
  req: PayloadRequest
}

declare module 'payload' {
  export interface DatabaseAdapter
    extends Omit<Args, 'idType' | 'logger' | 'migrationDir' | 'pool'>,
      DrizzleAdapter {
    beginTransaction: (options?: SQLiteTransactionConfig) => Promise<null | number | string>
    drizzle: Drizzle
    /**
     * An object keyed on each table, with a key value pair where the constraint name is the key, followed by the dot-notation field name
     * Used for returning properly formed errors from unique fields
     */
    fieldConstraints: Record<string, Record<string, string>>
    idType: Args['idType']
    initializing: Promise<void>
    localesSuffix?: string
    logger: DrizzleConfig['logger']
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
    tableNameMap: Map<string, string>
    transactionOptions: SQLiteTransactionConfig
    versionsSuffix?: string
  }
}
