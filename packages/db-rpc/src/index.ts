/**
 * @dotdo/db-rpc
 *
 * RPC client database adapter for Payload CMS
 * Connects to a remote db-rpc-server over HTTP or WebSocket using capnweb
 */

import type { DatabaseAdapterObj, Payload } from 'payload'

import { newHttpBatchRpcSession, newWebSocketRpcSession } from 'capnweb'
import { createDatabaseAdapter } from 'payload'

import type { AuthenticatedDatabaseApi, PublicDatabaseApi } from './interface.js'
import type { RpcAdapter, RpcAdapterArgs } from './types.js'

export type {
  AuthenticatedDatabaseApi,
  FindDistinctArgs,
  PublicDatabaseApi,
  ServerInfo,
} from './interface.js'
export type { RpcAdapter, RpcAdapterArgs } from './types.js'

/**
 * Create an RPC database adapter
 *
 * @example
 * ```typescript
 * import { rpcAdapter } from '@dotdo/db-rpc'
 *
 * export default buildConfig({
 *   db: rpcAdapter({
 *     url: 'https://db-server.example.com/rpc',
 *     token: process.env.DB_RPC_TOKEN,
 *   }),
 *   // ... rest of config
 * })
 * ```
 */
export function rpcAdapter(args: RpcAdapterArgs): DatabaseAdapterObj<RpcAdapter> {
  const { token, transport = 'http', url } = args

  function adapter({ payload }: { payload: Payload }): RpcAdapter {
    let serverInfo: RpcAdapter['serverInfo'] = null
    let connected = false

    /**
     * Get the token value (resolves function if needed)
     * Returns a Promise that resolves to the token string.
     */
    const resolveToken = (): Promise<string> => {
      const tokenValue = typeof token === 'function' ? token() : token
      return Promise.resolve(tokenValue)
    }

    /**
     * Create a fresh RPC session.
     *
     * With capnweb HTTP batch sessions, each batch should use a fresh session.
     * Awaiting any call (like getServerInfo) ends the batch, so we create
     * a new session for each operation.
     */
    const createSession = (): PublicDatabaseApi => {
      if (transport === 'websocket') {
        // @ts-expect-error - capnweb's types cause infinite recursion, runtime is correct
        return newWebSocketRpcSession(url)
      }
      // @ts-expect-error - capnweb's types cause infinite recursion, runtime is correct
      return newHttpBatchRpcSession(url)
    }

    /**
     * Helper to chain authenticate() with an operation.
     *
     * IMPORTANT: With capnweb HTTP batch, each batch uses a fresh session.
     * This helper creates a new session, chains authenticate() with the
     * operation, and returns the result.
     *
     *   ❌ Wrong: const api = await client.authenticate(token); await api.find()
     *   ✅ Right: await client.authenticate(token).find()
     *
     * Uses Awaited<T> to flatten nested promises since TypeScript doesn't
     * understand capnweb's thenable stubs that support method chaining.
     */
    const withAuth = <T>(operation: (auth: AuthenticatedDatabaseApi) => T): Promise<Awaited<T>> => {
      if (!connected) {
        return Promise.reject(new Error('RPC client not connected. Call connect() first.'))
      }
      // IMPORTANT: Create session INSIDE the .then() callback, AFTER token resolves.
      // capnweb starts a setTimeout(0) batch timer on session creation.
      // If we create the session before token resolution and the token takes >1 tick,
      // the batch sends empty and subsequent calls fail with "Batch RPC request ended."
      return resolveToken().then((tokenValue) => {
        const session = createSession()
        return operation(session.authenticate(tokenValue) as AuthenticatedDatabaseApi)
      }) as Promise<Awaited<T>>
    }

    return createDatabaseAdapter<RpcAdapter>({
      name: 'rpc',

      // RPC-specific properties
      authenticatedApi: null,
      client: null,
      serverInfo: null,

      // ============== Lifecycle ==============

      async connect() {
        // Fetch server info using a dedicated session
        const infoSession = createSession()
        serverInfo = await infoSession.getServerInfo()

        // Update adapter properties
        this.serverInfo = serverInfo
        connected = true
      },

      async destroy() {
        connected = false
        await Promise.resolve()
      },

      // ============== Transaction Methods ==============

      beginTransaction(options) {
        return withAuth((auth) => auth.beginTransaction(options))
      },

      async commitTransaction(id) {
        const txId = typeof id === 'string' ? id : String(await id)
        return withAuth((auth) => auth.commitTransaction(txId))
      },

      async rollbackTransaction(id) {
        const txId = typeof id === 'string' ? id : String(await id)
        return withAuth((auth) => auth.rollbackTransaction(txId))
      },

      // ============== Collection CRUD ==============

      find(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.find({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        ) as any
      },

      findOne(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.findOne({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        ) as any
      },

      create(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.create({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      updateOne(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.updateOne({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      updateMany(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.updateMany({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      deleteOne(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.deleteOne({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      deleteMany(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.deleteMany({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      count(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.count({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      upsert(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.upsert({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      findDistinct(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.findDistinct({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      queryDrafts(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.queryDrafts({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        ) as any
      },

      // ============== Globals ==============

      findGlobal(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.findGlobal({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        ) as any
      },

      createGlobal(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.createGlobal({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      updateGlobal(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.updateGlobal({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      // ============== Versions ==============

      findVersions(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.findVersions({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        ) as any
      },

      createVersion(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.createVersion({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      updateVersion(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.updateVersion({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      deleteVersions(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.deleteVersions({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      countVersions(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.countVersions({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      // ============== Global Versions ==============

      findGlobalVersions(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.findGlobalVersions({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        ) as any
      },

      createGlobalVersion(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.createGlobalVersion({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      updateGlobalVersion(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.updateGlobalVersion({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      countGlobalVersions(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.countGlobalVersions({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      // ============== Jobs ==============

      updateJobs(args) {
        const { req, ...rest } = args
        return withAuth((auth) =>
          auth.updateJobs({
            ...rest,
            transactionID: req?.transactionID as string | undefined,
          }),
        )
      },

      // ============== Migrations (not supported over RPC) ==============

      createMigration() {
        throw new Error(
          'Migrations must be run directly on the database server, not over RPC. ' +
            'Connect to the server directly to manage migrations.',
        )
      },

      migrate() {
        throw new Error(
          'Migrations must be run directly on the database server, not over RPC. ' +
            'Connect to the server directly to manage migrations.',
        )
      },

      migrateDown() {
        throw new Error(
          'Migrations must be run directly on the database server, not over RPC. ' +
            'Connect to the server directly to manage migrations.',
        )
      },

      migrateFresh() {
        throw new Error(
          'Migrations must be run directly on the database server, not over RPC. ' +
            'Connect to the server directly to manage migrations.',
        )
      },

      migrateRefresh() {
        throw new Error(
          'Migrations must be run directly on the database server, not over RPC. ' +
            'Connect to the server directly to manage migrations.',
        )
      },

      migrateReset() {
        throw new Error(
          'Migrations must be run directly on the database server, not over RPC. ' +
            'Connect to the server directly to manage migrations.',
        )
      },

      migrateStatus() {
        throw new Error(
          'Migrations must be run directly on the database server, not over RPC. ' +
            'Connect to the server directly to manage migrations.',
        )
      },

      // ============== Required Properties ==============

      defaultIDType: 'text',
      migrationDir: '',
      packageName: '@dotdo/db-rpc',
      payload,
    })
  }

  return {
    name: 'rpc',
    defaultIDType: 'text',
    init: adapter,
  }
}
