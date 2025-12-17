/**
 * Hono middleware for db-rpc-server
 *
 * Provides Hono middleware and server factories for exposing
 * a database adapter via RPC over HTTP and WebSocket.
 */

import type { BaseDatabaseAdapter, Payload, TypedUser } from 'payload'

import { newWorkersRpcResponse } from 'capnweb'
import { Hono } from 'hono'

import { DatabaseRpcTarget } from '../DatabaseRpcTarget.js'

/**
 * Token validation function type
 */
export type ValidateTokenFn = (token: string) => Promise<null | TypedUser>

/**
 * Options for creating RPC middleware
 */
export interface RpcMiddlewareOptions {
  /** The database adapter to expose via RPC */
  adapter: BaseDatabaseAdapter

  /**
   * The Payload instance for auth validation.
   * If provided, uses payload.auth() to validate tokens.
   * Either `payload` or `validateToken` must be provided.
   */
  payload?: Payload

  /**
   * Custom token validation function.
   * Use this when handling auth outside of Payload (e.g., Cloudflare Access, custom JWT).
   * Either `payload` or `validateToken` must be provided.
   *
   * @example
   * ```typescript
   * createRpcServer({
   *   adapter: db,
   *   validateToken: async (token) => {
   *     const decoded = await verifyJwt(token)
   *     return decoded ? { id: decoded.sub, email: decoded.email } : null
   *   },
   * })
   * ```
   */
  validateToken?: ValidateTokenFn
}

/**
 * Create Hono middleware that handles RPC requests
 *
 * This middleware handles both HTTP batch requests and WebSocket connections.
 * Use it to add RPC capabilities to an existing Hono application.
 *
 * @example
 * ```typescript
 * // With Payload auth
 * import { Hono } from 'hono'
 * import { createRpcMiddleware } from '@dotdo/db-rpc-server/hono'
 *
 * const app = new Hono()
 * app.route('/rpc', createRpcMiddleware({
 *   adapter: payload.db,
 *   payload,
 * }))
 * ```
 *
 * @example
 * ```typescript
 * // With custom auth
 * import { createRpcMiddleware } from '@dotdo/db-rpc-server/hono'
 *
 * app.route('/rpc', createRpcMiddleware({
 *   adapter: db,
 *   validateToken: async (token) => {
 *     const user = await myAuth.verify(token)
 *     return user
 *   },
 * }))
 * ```
 */
export function createRpcMiddleware(options: RpcMiddlewareOptions) {
  const { adapter, payload, validateToken } = options

  if (!payload && !validateToken) {
    throw new Error('Either payload or validateToken must be provided')
  }

  const target = new DatabaseRpcTarget(adapter, payload, validateToken)

  const app = new Hono()

  // Main RPC endpoint - handles both HTTP batch and WebSocket upgrade
  app.all('/', async (c) => {
    return newWorkersRpcResponse(c.req.raw, target)
  })

  // Health check endpoint
  app.get('/health', (c) => {
    return c.json({
      adapter: adapter.name,
      status: 'ok',
    })
  })

  return app
}

/**
 * Options for creating a standalone RPC server
 */
export interface RpcServerOptions extends RpcMiddlewareOptions {
  /** Base path for the RPC endpoint (default: '/rpc') */
  basePath?: string
}

/**
 * Create a standalone Hono server with RPC endpoint
 *
 * This creates a complete Hono application with the RPC middleware mounted.
 * Use it when you want a dedicated RPC server.
 *
 * @example
 * ```typescript
 * // Cloudflare Workers
 * import { createRpcServer } from '@dotdo/db-rpc-server/hono'
 *
 * export default createRpcServer({
 *   adapter: payload.db,
 *   payload,
 * })
 * ```
 *
 * @example
 * ```typescript
 * // Bun
 * import { createRpcServer } from '@dotdo/db-rpc-server/hono'
 *
 * const app = createRpcServer({
 *   adapter: payload.db,
 *   payload,
 * })
 *
 * export default {
 *   fetch: app.fetch,
 *   port: 3001,
 * }
 * ```
 */
export function createRpcServer(options: RpcServerOptions) {
  const { basePath = '/rpc', ...middlewareOptions } = options

  const app = new Hono()

  // Mount RPC middleware at the base path
  app.route(basePath, createRpcMiddleware(middlewareOptions))

  // Root health check
  app.get('/', (c) => {
    return c.json({
      name: '@dotdo/db-rpc-server',
      endpoints: {
        health: `${basePath}/health`,
        rpc: basePath,
      },
    })
  })

  return app
}
