/**
 * DatabaseRpcTarget
 *
 * The public (unauthenticated) RPC target that clients connect to.
 * Provides authentication and server info methods.
 */

import type { ServerInfo } from '@payloadcms/db-rpc/interface'
import type { BaseDatabaseAdapter, Payload } from 'payload'

import { RpcTarget } from 'capnweb'

import { AuthenticatedDatabaseTarget } from './AuthenticatedTarget.js'

/**
 * Public database RPC target
 *
 * This is the initial interface exposed to clients.
 * Clients must authenticate to get access to database operations.
 *
 * Note: We don't use `implements PublicDatabaseApi` to avoid strict type checking
 * issues with generic return types. The implementation matches at runtime.
 */
export class DatabaseRpcTarget extends RpcTarget {
  #adapter: BaseDatabaseAdapter
  #payload: Payload

  constructor(adapter: BaseDatabaseAdapter, payload: Payload) {
    super()
    this.#adapter = adapter
    this.#payload = payload
  }

  /**
   * Validate a bearer token using Payload's auth system
   *
   * Supports both JWT tokens and API keys.
   */
  async #validateToken(token: string) {
    try {
      // Use Payload's auth method to validate the token
      // This handles both JWT tokens and API keys
      const result = await this.#payload.auth({
        headers: new Headers({
          Authorization: `Bearer ${token}`,
        }),
      })

      return result.user
    } catch {
      // If auth fails, return null
      return null
    }
  }

  /**
   * Authenticate with a bearer token
   *
   * Validates the token using Payload's auth system and returns
   * an authenticated API stub bound to the validated user.
   *
   * @param token - Bearer token (JWT or API key)
   * @returns Authenticated database API
   * @throws Error if token is invalid
   */
  async authenticate(token: string): Promise<AuthenticatedDatabaseTarget> {
    const user = await this.#validateToken(token)

    if (!user) {
      throw new Error('Unauthorized: Invalid or expired token')
    }

    return new AuthenticatedDatabaseTarget(this.#adapter, this.#payload, user)
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
