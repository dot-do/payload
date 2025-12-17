/**
 * DatabaseRpcTarget
 *
 * The public (unauthenticated) RPC target that clients connect to.
 * Provides authentication and server info methods.
 */

import type { ServerInfo } from '@dotdo/db-rpc/interface'
import type { BaseDatabaseAdapter, Payload, TypedUser } from 'payload'

import { RpcTarget } from 'capnweb'

import { AuthenticatedDatabaseTarget } from './AuthenticatedTarget.js'

/**
 * Token validation function type
 */
export type ValidateTokenFn = (token: string) => Promise<null | TypedUser>

/**
 * Public database RPC target
 *
 * This is the initial interface exposed to clients.
 * Clients must authenticate to get access to database operations.
 *
 * Supports two authentication modes:
 * 1. Payload auth: Pass a Payload instance, uses payload.auth() to validate tokens
 * 2. Custom auth: Pass a validateToken callback for external auth systems
 *
 * Note: We don't use `implements PublicDatabaseApi` to avoid strict type checking
 * issues with generic return types. The implementation matches at runtime.
 */
export class DatabaseRpcTarget extends RpcTarget {
  #adapter: BaseDatabaseAdapter
  #payload?: Payload
  #validateTokenFn?: ValidateTokenFn

  constructor(adapter: BaseDatabaseAdapter, payload?: Payload, validateToken?: ValidateTokenFn) {
    super()
    this.#adapter = adapter
    this.#payload = payload
    this.#validateTokenFn = validateToken
  }

  /**
   * Validate a bearer token
   *
   * Uses either Payload's auth system or a custom validation function.
   */
  async #validateToken(token: string): Promise<null | TypedUser> {
    // Use custom validator if provided
    if (this.#validateTokenFn) {
      return this.#validateTokenFn(token)
    }

    // Otherwise use Payload's auth system
    if (this.#payload) {
      try {
        const result = await this.#payload.auth({
          headers: new Headers({
            Authorization: `Bearer ${token}`,
          }),
        })
        return result.user
      } catch {
        return null
      }
    }

    return null
  }

  /**
   * Authenticate with a bearer token
   *
   * Validates the token and returns an authenticated API stub bound to the validated user.
   *
   * @param token - Bearer token (JWT, API key, or custom token)
   * @returns Authenticated database API
   * @throws Error if token is invalid
   */
  async authenticate(token: string): Promise<AuthenticatedDatabaseTarget> {
    const user = await this.#validateToken(token)

    if (!user) {
      throw new Error('Unauthorized: Invalid or expired token')
    }

    return new AuthenticatedDatabaseTarget(this.#adapter, user, this.#payload)
  }

  /**
   * Get server metadata
   *
   * Returns information about the server and underlying database adapter.
   * This can be called without authentication.
   */
  getServerInfo(): ServerInfo {
    return {
      adapterName: this.#adapter.name,
      allowIDOnCreate: this.#adapter.allowIDOnCreate,
      defaultIDType: this.#adapter.defaultIDType,
    }
  }
}
