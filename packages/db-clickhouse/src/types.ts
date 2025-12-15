import type { ClickHouseClient } from '@clickhouse/client-web'
import type {
  BaseDatabaseAdapter,
  CollectionSlug,
  Document,
  Payload,
  PayloadRequest,
  TypeWithID,
  Where,
} from 'payload'

/**
 * Arguments for raw query execution
 */
export interface ExecuteArgs<T = unknown> {
  /** The SQL query to execute */
  query: string
  /** Query parameters for parameterized queries */
  query_params?: Record<string, unknown>
}

/**
 * Arguments for bulk upsert operation
 */
export interface UpsertManyArgs {
  /** Collection slug */
  collection: CollectionSlug
  /** Array of documents to upsert, each with data and where clause */
  docs: Array<{
    data: Record<string, unknown>
    where: Where
  }>
  /** Request context */
  req?: PayloadRequest
}

export interface ClickHouseAdapterArgs {
  /** Database name (default: 'default') */
  database?: string
  /** ID type for documents (default: 'text' - nanoid) */
  idType?: 'text' | 'uuid'
  /** Namespace to separate different Payload apps (default: 'payload') */
  namespace?: string
  /** ClickHouse password */
  password?: string
  /** Table name (default: 'data') */
  table?: string
  /** Timezone for DateTime handling (default: 'UTC'). Use 'auto' to detect from environment. */
  timezone?: string
  /** ClickHouse server URL (e.g., 'https://host:8443') */
  url: string
  /** ClickHouse username */
  username?: string
}

export type ClickHouseAdapter = {
  /**
   * ClickHouse client instance for direct database access
   * @example
   * ```ts
   * const result = await payload.db.clickhouse.query({
   *   query: 'SELECT * FROM data WHERE ns = {ns:String}',
   *   query_params: { ns: 'my-namespace' },
   *   format: 'JSONEachRow'
   * })
   * ```
   */
  clickhouse: ClickHouseClient | null
  /** Adapter configuration */
  config: ClickHouseAdapterArgs
  /** Database name */
  database: string
  /**
   * Execute a raw SQL query against ClickHouse
   * @example
   * ```ts
   * const rows = await payload.db.execute({
   *   query: 'SELECT count() FROM data WHERE ns = {ns:String}',
   *   query_params: { ns: 'my-namespace' }
   * })
   * ```
   */
  execute: <T = unknown>(args: ExecuteArgs<T>) => Promise<T[]>
  /** ID type for document IDs */
  idType: 'text' | 'uuid'
  /** Namespace for this adapter instance */
  namespace: string
  /** Table name */
  table: string
  /**
   * Bulk upsert operation - create or update multiple documents
   * @example
   * ```ts
   * const docs = await payload.db.upsertMany({
   *   collection: 'posts',
   *   docs: [
   *     { data: { title: 'Post 1' }, where: { slug: { equals: 'post-1' } } },
   *     { data: { title: 'Post 2' }, where: { slug: { equals: 'post-2' } } },
   *   ]
   * })
   * ```
   */
  upsertMany: (args: UpsertManyArgs) => Promise<Document[]>
} & BaseDatabaseAdapter

/**
 * Module augmentation to expose ClickHouse-specific properties on payload.db
 */
declare module 'payload' {
  export interface DatabaseAdapter extends Omit<ClickHouseAdapterArgs, 'password' | 'url'> {
    /**
     * ClickHouse client instance for direct database access
     */
    clickhouse: ClickHouseClient | null
    /**
     * Execute a raw SQL query against ClickHouse
     */
    execute: <T = unknown>(args: ExecuteArgs<T>) => Promise<T[]>
    /**
     * Bulk upsert operation - create or update multiple documents
     */
    upsertMany: (args: UpsertManyArgs) => Promise<Document[]>
  }
}

export interface DataRow {
  createdAt: string
  createdBy: null | string
  data: Record<string, unknown>
  deletedAt: null | string
  deletedBy: null | string
  id: string
  ns: string
  title: string
  type: string
  updatedAt: string
  updatedBy: null | string
  v: string // DateTime64(3) comes as ISO string
}

export interface InsertData {
  createdAt: number
  createdBy?: null | string
  data: Record<string, unknown>
  deletedAt?: null | number
  deletedBy?: null | string
  id: string
  ns: string
  title: string
  type: string
  updatedAt: number
  updatedBy?: null | string
  v: number // Date.now() timestamp
}

export interface QueryResult<T = DataRow> {
  rows: T[]
  totalDocs: number
}

export interface PaginatedResult<T = TypeWithID> {
  docs: T[]
  hasNextPage: boolean
  hasPrevPage: boolean
  limit: number
  nextPage: null | number
  page: number
  pagingCounter: number
  prevPage: null | number
  totalDocs: number
  totalPages: number
}

export type WhereOperator =
  | 'all'
  | 'contains'
  | 'equals'
  | 'exists'
  | 'greater_than'
  | 'greater_than_equal'
  | 'in'
  | 'less_than'
  | 'less_than_equal'
  | 'like'
  | 'near'
  | 'not_equals'
  | 'not_in'

export interface WhereCondition {
  [field: string]:
    | {
        [K in WhereOperator]?: unknown
      }
    | WhereCondition
    | WhereCondition[]
}

export interface OperationContext {
  adapter: ClickHouseAdapter
  payload?: Payload
  req?: PayloadRequest
}

export interface CreateArgs {
  collection: CollectionSlug
  data: Record<string, unknown>
  req?: PayloadRequest
}

export interface FindArgs {
  collection: CollectionSlug
  limit?: number
  page?: number
  pagination?: boolean
  req?: PayloadRequest
  sort?: string
  where?: WhereCondition
}

export interface FindOneArgs {
  collection: CollectionSlug
  req?: PayloadRequest
  where: WhereCondition
}

export interface UpdateOneArgs {
  collection: CollectionSlug
  data: Record<string, unknown>
  req?: PayloadRequest
  where: WhereCondition
}

export interface UpdateManyArgs {
  collection: CollectionSlug
  data: Record<string, unknown>
  req?: PayloadRequest
  where: WhereCondition
}

export interface DeleteOneArgs {
  collection: CollectionSlug
  req?: PayloadRequest
  where: WhereCondition
}

export interface DeleteManyArgs {
  collection: CollectionSlug
  req?: PayloadRequest
  where: WhereCondition
}

export interface CountArgs {
  collection: CollectionSlug
  req?: PayloadRequest
  where?: WhereCondition
}

export interface FindDistinctArgs {
  collection: CollectionSlug
  field: string
  req?: PayloadRequest
  where?: WhereCondition
}

// Global types
export interface GlobalArgs {
  req?: PayloadRequest
  slug: string
}

export interface CreateGlobalArgs extends GlobalArgs {
  data: Record<string, unknown>
}

export interface UpdateGlobalArgs extends GlobalArgs {
  data: Record<string, unknown>
}

// Version types
export interface VersionArgs {
  collection: CollectionSlug
  parent: string // Document ID
  req?: PayloadRequest
}

export interface CreateVersionArgs extends VersionArgs {
  autosave?: boolean
  versionData: Record<string, unknown>
}

export interface FindVersionsArgs extends VersionArgs {
  limit?: number
  page?: number
  pagination?: boolean
  sort?: string
  where?: WhereCondition
}

export interface UpdateVersionArgs {
  collection: CollectionSlug
  id: string // Version ID
  req?: PayloadRequest
  versionData: Record<string, unknown>
}

export interface DeleteVersionsArgs extends VersionArgs {
  where?: WhereCondition
}

export interface CountVersionsArgs extends VersionArgs {
  where?: WhereCondition
}

// Global version types
export interface GlobalVersionArgs {
  global: string
  req?: PayloadRequest
}

export interface CreateGlobalVersionArgs extends GlobalVersionArgs {
  autosave?: boolean
  versionData: Record<string, unknown>
}

export interface FindGlobalVersionsArgs extends GlobalVersionArgs {
  limit?: number
  page?: number
  pagination?: boolean
  sort?: string
  where?: WhereCondition
}

export interface UpdateGlobalVersionArgs {
  global: string
  id: string
  req?: PayloadRequest
  versionData: Record<string, unknown>
}

export interface CountGlobalVersionsArgs extends GlobalVersionArgs {
  where?: WhereCondition
}
