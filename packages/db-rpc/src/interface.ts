/**
 * Shared RPC interface definitions for db-rpc client and server
 *
 * These interfaces define the contract between the client adapter and the server.
 * The server implements these interfaces via RpcTarget classes.
 * The client uses these as type parameters for RpcStub proxies.
 */

import type {
  CollectionSlug,
  CountArgs,
  CountGlobalVersionArgs,
  CreateArgs,
  CreateGlobalArgs,
  CreateGlobalVersionArgs,
  CreateVersionArgs,
  DeleteManyArgs,
  DeleteOneArgs,
  DeleteVersionsArgs,
  FindArgs,
  FindGlobalArgs,
  FindGlobalVersionsArgs,
  FindOneArgs,
  FindVersionsArgs,
  Job,
  JsonObject,
  PaginatedDistinctDocs,
  PaginatedDocs,
  PayloadRequest,
  QueryDraftsArgs,
  Sort,
  TypeWithID,
  TypeWithVersion,
  UpdateGlobalArgs,
  UpdateGlobalVersionArgs,
  UpdateJobsArgs,
  UpdateManyArgs,
  UpdateOneArgs,
  UpdateVersionArgs,
  UpsertArgs,
  Where,
} from 'payload'

/**
 * Arguments for findDistinct operation
 * Defined locally since not exported from payload main index
 */
export type FindDistinctArgs = {
  collection: CollectionSlug
  field: string
  limit?: number
  locale?: string
  page?: number
  req?: Partial<PayloadRequest>
  sort?: Sort
  where?: Where
}

/**
 * Server metadata returned by getServerInfo
 */
export interface ServerInfo {
  /** The name of the underlying database adapter (e.g., 'mongoose', 'postgres') */
  adapterName: string
  /** Whether the server's adapter allows custom IDs on create */
  allowIDOnCreate?: boolean
  /** The default ID type for the underlying database */
  defaultIDType: 'number' | 'text'
}

/**
 * Public (unauthenticated) database API
 *
 * This is the initial interface exposed by the server.
 * Clients must call authenticate() to get an AuthenticatedDatabaseApi stub.
 */
export interface PublicDatabaseApi {
  /**
   * Authenticate with a bearer token (JWT or API key)
   * Returns an authenticated API stub bound to the validated user
   */
  authenticate(token: string): AuthenticatedDatabaseApi | Promise<AuthenticatedDatabaseApi>

  /**
   * Get server metadata without authentication
   * Useful for clients to discover server capabilities
   */
  getServerInfo(): Promise<ServerInfo>
}

/**
 * Arguments for operations that may include a transaction ID
 */
type WithTransaction<T> = {
  /** Transaction ID from beginTransaction, if operating within a transaction */
  transactionID?: string
} & T

/**
 * Omit 'req' from args since it's handled server-side
 */
type OmitReq<T> = Omit<T, 'req'>

/**
 * Authenticated database API
 *
 * All database operations are available through this interface.
 * The server binds the authenticated user to all operations.
 */
export interface AuthenticatedDatabaseApi {
  // ============== Transaction Methods ==============

  /**
   * Start a transaction
   * @returns Transaction ID or null if transactions not supported
   */
  beginTransaction(options?: Record<string, unknown>): Promise<null | string>

  /**
   * Commit a transaction
   * @param txId - Transaction ID from beginTransaction
   */
  commitTransaction(txId: string): Promise<void>

  /**
   * Count documents
   */
  count(args: WithTransaction<OmitReq<CountArgs>>): Promise<{ totalDocs: number }>

  // ============== Collection CRUD ==============

  /**
   * Count global versions
   */
  countGlobalVersions(
    args: WithTransaction<OmitReq<CountGlobalVersionArgs>>,
  ): Promise<{ totalDocs: number }>

  /**
   * Count versions
   */
  countVersions(args: WithTransaction<OmitReq<CountArgs>>): Promise<{ totalDocs: number }>

  /**
   * Create a document
   */
  create(args: WithTransaction<OmitReq<CreateArgs>>): Promise<TypeWithID>

  /**
   * Create a global document
   */
  createGlobal<T extends Record<string, unknown> = Record<string, unknown>>(
    args: WithTransaction<OmitReq<CreateGlobalArgs<T>>>,
  ): Promise<T>

  /**
   * Create a global version
   */
  createGlobalVersion<T extends JsonObject = JsonObject>(
    args: WithTransaction<OmitReq<CreateGlobalVersionArgs<T>>>,
  ): Promise<Omit<TypeWithVersion<T>, 'parent'>>

  /**
   * Create a version
   */
  createVersion<T extends JsonObject = JsonObject>(
    args: WithTransaction<OmitReq<CreateVersionArgs<T>>>,
  ): Promise<TypeWithVersion<T>>

  /**
   * Delete multiple documents
   */
  deleteMany(args: WithTransaction<OmitReq<DeleteManyArgs>>): Promise<void>

  /**
   * Delete a single document
   */
  deleteOne(args: WithTransaction<OmitReq<DeleteOneArgs>>): Promise<TypeWithID>

  /**
   * Delete versions
   */
  deleteVersions(args: WithTransaction<OmitReq<DeleteVersionsArgs>>): Promise<void>

  /**
   * Find documents in a collection
   */
  find<T = TypeWithID>(args: WithTransaction<OmitReq<FindArgs>>): Promise<PaginatedDocs<T>>

  /**
   * Find distinct values for a field
   */
  findDistinct(
    args: WithTransaction<OmitReq<FindDistinctArgs>>,
  ): Promise<PaginatedDistinctDocs<Record<string, unknown>>>

  // ============== Globals ==============

  /**
   * Find a global document
   */
  findGlobal<T extends Record<string, unknown> = Record<string, unknown>>(
    args: WithTransaction<OmitReq<FindGlobalArgs>>,
  ): Promise<T>

  /**
   * Find versions of a global
   */
  findGlobalVersions<T = JsonObject>(
    args: WithTransaction<OmitReq<FindGlobalVersionsArgs>>,
  ): Promise<PaginatedDocs<TypeWithVersion<T>>>

  /**
   * Find a single document
   */
  findOne<T extends TypeWithID = TypeWithID>(
    args: WithTransaction<OmitReq<FindOneArgs>>,
  ): Promise<null | T>

  // ============== Versions ==============

  /**
   * Find versions of documents
   */
  findVersions<T = JsonObject>(
    args: WithTransaction<OmitReq<FindVersionsArgs>>,
  ): Promise<PaginatedDocs<TypeWithVersion<T>>>

  /**
   * Query draft documents
   */
  queryDrafts<T = TypeWithID>(
    args: WithTransaction<OmitReq<QueryDraftsArgs>>,
  ): Promise<PaginatedDocs<T>>

  /**
   * Rollback a transaction
   * @param txId - Transaction ID from beginTransaction
   */
  rollbackTransaction(txId: string): Promise<void>

  /**
   * Update a global document
   */
  updateGlobal<T extends Record<string, unknown> = Record<string, unknown>>(
    args: WithTransaction<OmitReq<UpdateGlobalArgs<T>>>,
  ): Promise<T>

  /**
   * Update a global version
   */
  updateGlobalVersion<T extends JsonObject = JsonObject>(
    args: WithTransaction<OmitReq<UpdateGlobalVersionArgs<T>>>,
  ): Promise<TypeWithVersion<T>>

  // ============== Global Versions ==============

  /**
   * Update jobs
   */
  updateJobs(args: WithTransaction<OmitReq<UpdateJobsArgs>>): Promise<Job[] | null>

  /**
   * Update multiple documents
   */
  updateMany(args: WithTransaction<OmitReq<UpdateManyArgs>>): Promise<null | TypeWithID[]>

  /**
   * Update a single document
   */
  updateOne(args: WithTransaction<OmitReq<UpdateOneArgs>>): Promise<TypeWithID>

  /**
   * Update a version
   */
  updateVersion<T extends JsonObject = JsonObject>(
    args: WithTransaction<OmitReq<UpdateVersionArgs<T>>>,
  ): Promise<TypeWithVersion<T>>

  // ============== Jobs ==============

  /**
   * Upsert a document (create or update)
   */
  upsert(args: WithTransaction<OmitReq<UpsertArgs>>): Promise<TypeWithID>
}
