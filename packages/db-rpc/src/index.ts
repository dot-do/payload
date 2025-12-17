/**
 * @payloadcms/db-rpc
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
 * import { rpcAdapter } from '@payloadcms/db-rpc'
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
    let authenticatedApi: AuthenticatedDatabaseApi | null = null
    let serverInfo: RpcAdapter['serverInfo'] = null

    const getToken = async (): Promise<string> => {
      if (typeof token === 'function') {
        return token()
      }
      return token
    }

    const getAuthenticatedApi = async (): Promise<AuthenticatedDatabaseApi> => {
      if (!client) {
        throw new Error('RPC client not connected. Call connect() first.')
      }
      if (!authenticatedApi) {
        const tokenValue = await getToken()
        // authenticate() may return a Promise (server-side) or direct stub (capnweb)
        const result = client.authenticate(tokenValue)
        authenticatedApi = result instanceof Promise ? await result : result
      }
      return authenticatedApi
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
          authenticatedApi = null
        }
        await Promise.resolve()
      },

      // ============== Transaction Methods ==============

      async beginTransaction(options) {
        const api = await getAuthenticatedApi()
        const txId = await api.beginTransaction(options)
        return txId
      },

      async commitTransaction(id) {
        const api = await getAuthenticatedApi()
        const txId = typeof id === 'string' ? id : String(await id)
        await api.commitTransaction(txId)
      },

      async rollbackTransaction(id) {
        const api = await getAuthenticatedApi()
        const txId = typeof id === 'string' ? id : String(await id)
        await api.rollbackTransaction(txId)
      },

      // ============== Collection CRUD ==============

      async find(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.find({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async findOne(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.findOne({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async create(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.create({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async updateOne(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.updateOne({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async updateMany(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.updateMany({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async deleteOne(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.deleteOne({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async deleteMany(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        await api.deleteMany({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async count(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.count({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async upsert(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.upsert({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async findDistinct(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.findDistinct({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async queryDrafts(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.queryDrafts({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      // ============== Globals ==============

      async findGlobal(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.findGlobal({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async createGlobal(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.createGlobal({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async updateGlobal(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.updateGlobal({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      // ============== Versions ==============

      async findVersions(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.findVersions({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async createVersion(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.createVersion({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async updateVersion(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.updateVersion({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async deleteVersions(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        await api.deleteVersions({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async countVersions(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.countVersions({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      // ============== Global Versions ==============

      async findGlobalVersions(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.findGlobalVersions({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async createGlobalVersion(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.createGlobalVersion({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async updateGlobalVersion(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.updateGlobalVersion({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      async countGlobalVersions(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.countGlobalVersions({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
      },

      // ============== Jobs ==============

      async updateJobs(args) {
        const api = await getAuthenticatedApi()
        const { req, ...rest } = args
        return api.updateJobs({
          ...rest,
          transactionID: req?.transactionID as string | undefined,
        })
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
      packageName: '@payloadcms/db-rpc',
      payload,
    })
  }

  return {
    name: 'rpc',
    defaultIDType: 'text',
    init: adapter,
  }
}
