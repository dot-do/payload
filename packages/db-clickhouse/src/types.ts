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
 * Generic ClickHouse client interface that works with both
 * @clickhouse/client-web and chdb-wrapped clients
 */
export interface ClickHouseClientLike {
  close(): Promise<void>
  command(params: { query: string; query_params?: Record<string, unknown> }): Promise<unknown>
  query<T = unknown>(params: {
    format?: string
    query: string
    query_params?: Record<string, unknown>
  }): Promise<{ json: <R = T>() => Promise<R[]> }>
}

/**
 * Arguments for raw query execution
 */
export interface ExecuteArgs<_T = unknown> {
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
  /** Array of documents to upsert. Include `id` in data to update existing, omit for new. */
  docs: Array<{
    data: Record<string, unknown>
  }>
  /** Request context */
  req?: PayloadRequest
}

/**
 * Vector index configuration for similarity search
 */
export interface VectorIndexConfig {
  /** Enable vector index on the search table (default: false) */
  enabled: boolean
  /**
   * Distance metric for similarity search
   * - 'L2Distance': Euclidean distance (default)
   * - 'cosineDistance': Cosine similarity
   */
  metric?: 'cosineDistance' | 'L2Distance'
}

/**
 * chdb Session interface - matches the Session class from 'chdb' package
 */
export interface ChdbSession {
  cleanup(): void
  query(query: string, format?: string): string
}

export interface ClickHouseAdapterArgs {
  /**
   * Pre-configured ClickHouse client instance.
   * If provided, url/username/password are ignored and the client is used directly.
   *
   * @example
   * ```ts
   * import { createClient } from '@clickhouse/client-web'
   * import { clickhouseAdapter } from '@dotdo/db-clickhouse'
   *
   * const client = createClient({ url: '...', username: '...', password: '...' })
   * const adapter = clickhouseAdapter({ client, database: 'mydb' })
   * ```
   */
  client?: ClickHouseClientLike
  /** Database name (default: 'default') */
  database?: string
  /** Default transaction timeout in ms (default: 30000, null for no timeout) */
  defaultTransactionTimeout?: null | number
  /** Embedding dimensions for vector search (default: 1536) */
  embeddingDimensions?: number
  /** ID type for documents (default: 'text' - nanoid) */
  idType?: 'text' | 'uuid'
  /** Namespace to separate different Payload apps (default: 'payload') */
  namespace?: string
  /** ClickHouse password */
  password?: string
  /**
   * chdb Session instance for embedded ClickHouse.
   * If provided, it will be wrapped in a ChdbClient automatically.
   *
   * @example
   * ```ts
   * import { Session } from 'chdb'
   * import { clickhouseAdapter } from '@dotdo/db-clickhouse'
   *
   * const session = new Session('/path/to/data')
   * const adapter = clickhouseAdapter({ session })
   * ```
   */
  session?: ChdbSession
  /** Table name (default: 'data') */
  table?: string
  /** Timezone for DateTime handling (default: 'UTC'). Use 'auto' to detect from environment. */
  timezone?: string
  /** ClickHouse server URL (e.g., 'https://host:8443'). Required if client is not provided. */
  url?: string
  /** ClickHouse username */
  username?: string
  /** Vector index configuration for similarity search (default: disabled) */
  vectorIndex?: VectorIndexConfig
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
  /** Default transaction timeout in ms */
  defaultTransactionTimeout: null | number
  /** Embedding dimensions for vector search */
  embeddingDimensions: number
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
  /**
   * Get pending items from the search queue for external embedding generation
   * @example
   * ```ts
   * const items = await payload.db.getSearchQueue({ limit: 50 })
   * // items = [{ id, collection, docId, text }, ...]
   * ```
   */
  getSearchQueue: (args?: GetSearchQueueArgs) => Promise<SearchQueueItem[]>
  /** ID type for document IDs */
  idType: 'text' | 'uuid'
  /**
   * Log an event to the events table
   * @example
   * ```ts
   * const eventId = await payload.db.logEvent({
   *   type: 'doc.create',
   *   collection: 'posts',
   *   docId: 'post-id',
   *   userId: 'user-id',
   *   input: { title: 'New Post' },
   *   result: { success: true }
   * })
   * ```
   */
  logEvent: (args: LogEventArgs) => Promise<string>
  /** Namespace for this adapter instance */
  namespace: string
  /**
   * Query events from the events table
   * @example
   * ```ts
   * const result = await payload.db.queryEvents({
   *   where: { type: { equals: 'doc.create' } },
   *   limit: 50,
   *   page: 1,
   *   sort: '-timestamp'
   * })
   * ```
   */
  queryEvents: (args?: QueryEventsArgs) => Promise<QueryEventsResult>
  /**
   * Perform full-text, vector, or hybrid search across the search table
   * @example
   * ```ts
   * // Text-only search
   * const results = await payload.db.search({
   *   text: 'my search query',
   *   limit: 10
   * })
   *
   * // Vector similarity search
   * const results = await payload.db.search({
   *   vector: [0.1, 0.2, ...],
   *   limit: 10
   * })
   *
   * // Hybrid search
   * const results = await payload.db.search({
   *   text: 'my query',
   *   vector: [0.1, 0.2, ...],
   *   hybrid: { textWeight: 0.7, vectorWeight: 0.3 },
   *   limit: 10
   * })
   * ```
   */
  search: (args?: SearchArgs) => Promise<SearchResult>
  /**
   * Sync a document to the search table for full-text search
   * @example
   * ```ts
   * const searchId = await payload.db.syncToSearch({
   *   collection: 'posts',
   *   doc: { id: 'doc-id', title: 'My Post', content: 'Content...' },
   *   chunkIndex: 0
   * })
   * ```
   */
  syncToSearch: (args: SyncToSearchArgs) => Promise<string>
  /** Table name */
  table: string
  /**
   * Update the status of a search item after embedding generation
   * @example
   * ```ts
   * await payload.db.updateSearchStatus({
   *   id: 'search-item-id',
   *   status: 'ready',
   *   embedding: [0.1, 0.2, 0.3, ...]
   * })
   * ```
   */
  updateSearchStatus: (args: UpdateSearchStatusArgs) => Promise<void>
  /**
   * Bulk upsert operation - insert multiple documents in a single batch
   * @example
   * ```ts
   * const docs = await payload.db.upsertMany({
   *   collection: 'posts',
   *   docs: [
   *     { data: { title: 'Post 1', slug: 'post-1' } },
   *     { data: { id: 'existing-id', title: 'Updated Post' } },
   *   ]
   * })
   * ```
   */
  upsertMany: (args: UpsertManyArgs) => Promise<Document[]>
  /** Vector index configuration */
  vectorIndex?: VectorIndexConfig
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
     * Get pending items from the search queue for external embedding generation
     */
    getSearchQueue: (args?: GetSearchQueueArgs) => Promise<SearchQueueItem[]>
    /**
     * Log an event to the events table
     */
    logEvent: (args: LogEventArgs) => Promise<string>
    /**
     * Query events from the events table
     */
    queryEvents: (args?: QueryEventsArgs) => Promise<QueryEventsResult>
    /**
     * Perform full-text, vector, or hybrid search across the search table
     */
    search: (args?: SearchArgs) => Promise<SearchResult>
    /**
     * Sync a document to the search table for full-text search
     */
    syncToSearch: (args: SyncToSearchArgs) => Promise<string>
    /**
     * Update the status of a search item after embedding generation
     */
    updateSearchStatus: (args: UpdateSearchStatusArgs) => Promise<void>
    /**
     * Bulk upsert operation - insert multiple documents in a single batch
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

export interface RelationshipRow {
  deletedAt: null | number
  fromField: string
  fromId: string
  fromType: string
  locale: null | string
  ns: string
  position: number
  toId: string
  toType: string
  v: number // timestamp in milliseconds
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

// Search sync types
export interface SyncToSearchArgs {
  chunkIndex?: number
  collection: string
  doc: Record<string, unknown>
}

export interface SearchArgs {
  hybrid?: {
    textWeight: number
    vectorWeight: number
  }
  limit?: number
  text?: string
  vector?: number[]
  where?: Where
}

export interface SearchResultDoc {
  chunkIndex: number
  collection: string
  docId: string
  id: string
  score: number
  text: string
}

export interface SearchResult {
  docs: SearchResultDoc[]
}

export interface GetSearchQueueArgs {
  limit?: number
}

export interface SearchQueueItem {
  collection: string
  docId: string
  id: string
  text: string
}

export interface UpdateSearchStatusArgs {
  embedding?: number[]
  error?: string
  id: string
  status: 'failed' | 'ready'
}

// Event log types
export interface LogEventArgs {
  collection?: string
  docId?: string
  duration?: number
  input?: Record<string, unknown>
  ip?: string
  result?: Record<string, unknown>
  sessionId?: string
  type: string
  userId?: string
}

export interface QueryEventsArgs {
  limit?: number
  page?: number
  sort?: string
  where?: Where
}

export interface EventRow {
  collection: null | string
  docId: null | string
  duration: number
  id: string
  input: Record<string, unknown>
  ip: null | string
  ns: string
  result: Record<string, unknown>
  sessionId: null | string
  timestamp: string
  type: string
  userId: null | string
}

export interface QueryEventsResult {
  docs: EventRow[]
  hasNextPage: boolean
  hasPrevPage: boolean
  limit: number
  nextPage: null | number
  page: number
  prevPage: null | number
  totalDocs: number
  totalPages: number
}

// Transaction types
export interface BeginTransactionArgs {
  timeout?: null | number
}

// Migration types
export interface MigrateUpArgs {
  payload: Payload
  req?: PayloadRequest
}

export interface MigrateDownArgs {
  payload: Payload
  req?: PayloadRequest
}
