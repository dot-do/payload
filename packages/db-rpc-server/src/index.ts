/**
 * @dotdo/db-rpc-server
 *
 * RPC server for Payload database adapters
 * Exposes any database adapter via HTTP/WebSocket using capnweb
 */

// Core exports
export { AuthenticatedDatabaseTarget } from './AuthenticatedTarget.js'
export { DatabaseRpcTarget } from './DatabaseRpcTarget.js'

// Middleware exports
export { createRpcMiddleware, createRpcServer } from './middleware/hono.js'
export type { RpcMiddlewareOptions, RpcServerOptions, ValidateTokenFn } from './middleware/hono.js'

// Type exports
export type { RpcContext, RpcServerOptions as RpcOptions } from './types.js'

// Re-export interface types from db-rpc
export type {
  AuthenticatedDatabaseApi,
  PublicDatabaseApi,
  ServerInfo,
} from '@dotdo/db-rpc/interface'
