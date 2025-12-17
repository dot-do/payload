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
    let client: null | PublicDatabaseApi = null
    let serverInfo: RpcAdapter['serverInfo'] = null

    /**
     * Get the token value (resolves function if needed)
     * Returns a Promise that resolves to the token string.
     */
    const resolveToken = (): Promise<string> => {
      const tokenValue = typeof token === 'function' ? token() : token
      return Promise.resolve(tokenValue)
    }

    /**
     * Helper to chain authenticate() with an operation.
     *
     * IMPORTANT: With capnweb, you must NOT await intermediate RPC calls.
     * This helper ensures proper promise pipelining by returning a single
     * chained promise without intermediate awaits.
     *
     *   ❌ Wrong: const api = await client.authenticate(token); await api.find()
     *   ✅ Right: await client.authenticate(token).find()
     *
     * Uses Awaited<T> to flatten nested promises since TypeScript doesn't
     * understand capnweb's thenable stubs that support method chaining.
     */
    const withAuth = <T>(operation: (auth: AuthenticatedDatabaseApi) => T): Promise<Awaited<T>> => {
      if (!client) {
        return Promise.reject(new Error('RPC client not connected. Call connect() first.'))
      }
      // Chain token resolution -> authenticate -> operation in a single promise
      // DO NOT await authenticate() - chain directly to preserve pipelining
      // capnweb stubs are thenable AND have methods - TypeScript doesn't understand this
      return resolveToken().then((tokenValue) =>
        operation(client!.authenticate(tokenValue) as AuthenticatedDatabaseApi),
      ) as Promise<Awaited<T>>
    }

    return createDatabaseAdapter<RpcAdapter>({
      name: 'rpc',

      // RPC-specific properties
      authenticatedApi: null,
      client: null,
      serverInfo: null,

      // ============== Lifecycle ==============

      async connect() {
        // capnweb returns complex stub types with recursive generics
        // We break the type inference by avoiding direct generic instantiation
        if (transport === 'websocket') {
          // @ts-expect-error - capnweb's types cause infinite recursion, runtime is correct
          client = newWebSocketRpcSession(url)
        } else {
          // @ts-expect-error - capnweb's types cause infinite recursion, runtime is correct
          client = newHttpBatchRpcSession(url)
        }

        // Fetch server info
        serverInfo = await (client as PublicDatabaseApi).getServerInfo()

        // Update adapter properties
        this.client = client
        this.serverInfo = serverInfo
      },

      async destroy() {
        if (client) {
          // Dispose of the RPC session
          ;(client as unknown as { [Symbol.dispose](): void })[Symbol.dispose]?.()
          client = null
        }
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
