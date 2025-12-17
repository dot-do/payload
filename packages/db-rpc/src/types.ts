/**
 * Types for the db-rpc client adapter
 */

import type { BaseDatabaseAdapter, Payload } from 'payload'

import type { AuthenticatedDatabaseApi, PublicDatabaseApi } from './interface.js'

/**
 * Transport type for RPC communication
 */
export type TransportType = 'http' | 'websocket'

/**
 * Arguments for creating an RPC adapter
 */
export interface RpcAdapterArgs {
  /**
   * Bearer token for authentication
   * Can be a string or a function that returns a token (sync or async)
   * The function form is useful for refreshing tokens
   */
  token: (() => Promise<string> | string) | string

  /**
   * Transport type for RPC communication
   * - 'http': Uses HTTP batch mode (default) - good for most use cases
   * - 'websocket': Uses persistent WebSocket connection - good for real-time or high-frequency operations
   * @default 'http'
   */
  transport?: TransportType

  /**
   * RPC server URL
   * For HTTP: Use http:// or https://
   * For WebSocket: Use ws:// or wss://
   */
  url: string
}

/**
 * Extended adapter type with RPC-specific properties
 */
export interface RpcAdapter extends BaseDatabaseAdapter {
  /** The authenticated API stub (after connect) */
  authenticatedApi: AuthenticatedDatabaseApi | null
  /** The underlying RPC client */
  client: null | PublicDatabaseApi
  /** Reference to the Payload instance */
  payload: Payload
  /** Server information (after connect) */
  serverInfo: {
    adapterName: string
    allowIDOnCreate?: boolean
    defaultIDType: 'number' | 'text'
  } | null
}
