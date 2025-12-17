/**
 * Types for the db-rpc-server package
 */

import type { BaseDatabaseAdapter, Payload, TypedUser } from 'payload'

/**
 * Options for creating an RPC server or middleware
 */
export interface RpcServerOptions {
  /** The database adapter to wrap */
  adapter: BaseDatabaseAdapter

  /** The Payload instance for auth validation */
  payload: Payload

  /**
   * Transaction timeout in milliseconds
   * Transactions that are not committed or rolled back within this time will be automatically rolled back
   * @default 30000
   */
  transactionTimeout?: number
}

/**
 * Context passed to RPC handlers
 */
export interface RpcContext {
  /** The database adapter */
  adapter: BaseDatabaseAdapter

  /** The Payload instance */
  payload: Payload

  /** The authenticated user (if authenticated) */
  user?: TypedUser
}
