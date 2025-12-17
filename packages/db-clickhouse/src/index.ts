import type { DatabaseAdapterObj, Payload } from 'payload'

import path from 'path'
import { createDatabaseAdapter } from 'payload'

import type { ClickHouseAdapter, ClickHouseAdapterArgs } from './types.js'

import { connect } from './connect.js'
import { createMigration } from './createMigration.js'
import { destroy } from './destroy.js'
import { init } from './init.js'
import { migrate } from './migrate.js'
import { migrateFresh } from './migrateFresh.js'
import {
  beginTransaction,
  commitTransaction,
  count,
  countGlobalVersions,
  countVersions,
  create,
  createGlobal,
  createGlobalVersion,
  createVersion,
  deleteMany,
  deleteOne,
  deleteVersions,
  execute,
  find,
  findDistinct,
  findGlobal,
  findGlobalVersions,
  findOne,
  findVersions,
  getSearchQueue,
  logEvent,
  queryDrafts,
  queryEvents,
  rollbackTransaction,
  search,
  syncToSearch,
  updateGlobal,
  updateGlobalVersion,
  updateMany,
  updateOne,
  updateSearchStatus,
  updateVersion,
  upsert,
  upsertMany,
} from './operations/index.js'
import { assertValidNamespace } from './utilities/sanitize.js'

export { ChdbClient } from './local/chdbClient.js'

export type {
  ChdbSession,
  ClickHouseAdapter,
  ClickHouseAdapterArgs,
  ClickHouseClientLike,
  EventRow,
  ExecuteArgs,
  GetSearchQueueArgs,
  LogEventArgs,
  MigrateDownArgs,
  MigrateUpArgs,
  QueryEventsArgs,
  QueryEventsResult,
  SearchArgs,
  SearchQueueItem,
  SearchResult,
  SearchResultDoc,
  SyncToSearchArgs,
  UpdateSearchStatusArgs,
  UpsertManyArgs,
  VectorIndexConfig,
} from './types.js'

export { assertValidNamespace, validateNamespace } from './utilities/sanitize.js'

/**
 * Create a ClickHouse database adapter for Payload CMS
 *
 * @example
 * ```typescript
 * import { clickhouseAdapter } from '@dotdo/db-clickhouse'
 *
 * export default buildConfig({
 *   db: clickhouseAdapter({
 *     url: 'https://your-clickhouse-host:8443',
 *     username: 'default',
 *     password: 'your-password',
 *     database: 'myapp',
 *     namespace: 'production'
 *   }),
 *   // ...
 * })
 * ```
 */
export function clickhouseAdapter(args: ClickHouseAdapterArgs): DatabaseAdapterObj {
  const {
    client,
    database = 'default',
    defaultTransactionTimeout = 30_000,
    embeddingDimensions = 1536,
    idType = 'text',
    namespace = 'payload',
    password = '',
    session,
    table = 'data',
    url,
    username = 'default',
    vectorIndex,
  } = args

  // Validate that either client, session, or url is provided
  if (!client && !session && !url) {
    throw new Error('ClickHouse adapter requires either a client, session, or url to be provided')
  }

  // Validate namespace (allows dots for domain names)
  assertValidNamespace(namespace)

  function adapter({ payload }: { payload: Payload }) {
    return createDatabaseAdapter<ClickHouseAdapter>({
      name: 'clickhouse',
      defaultIDType: 'text',
      packageName: '@dotdo/db-clickhouse',

      // ClickHouse-specific config
      clickhouse: null, // Set during connect, exposes raw client via payload.db.clickhouse
      config: {
        client,
        database,
        defaultTransactionTimeout,
        embeddingDimensions,
        idType,
        namespace,
        password,
        session,
        table,
        url,
        username,
        vectorIndex,
      },
      database,
      defaultTransactionTimeout,
      embeddingDimensions,
      idType,
      namespace,
      table,
      vectorIndex,

      // Core adapter
      payload,

      // Connection
      connect,
      destroy,
      init,

      // CRUD operations
      count,
      create,
      deleteMany,
      deleteOne,
      find,
      findDistinct,
      findOne,
      updateMany,
      updateOne,

      // Global operations
      createGlobal,
      findGlobal,
      updateGlobal,

      // Version operations
      countVersions,
      createVersion,
      deleteVersions,
      findVersions,
      updateVersion,

      // Global version operations
      countGlobalVersions,
      createGlobalVersion,
      findGlobalVersions,
      updateGlobalVersion,

      // Draft queries
      queryDrafts,

      // Upsert operations
      upsert,
      upsertMany,

      // Search operations
      getSearchQueue,
      search,
      syncToSearch,
      updateSearchStatus,

      // Event logging
      logEvent,
      queryEvents,

      // Raw query execution
      execute,

      // Transaction operations - uses actions table for transaction staging
      beginTransaction,
      commitTransaction,
      rollbackTransaction,

      // Migration support
      createMigration,
      migrate,
      migrateFresh,
      async migrateRefresh(this: ClickHouseAdapter) {
        // Delete all data and re-run migrations
        if (this.clickhouse) {
          await this.clickhouse.command({
            query: `DELETE FROM ${this.table} WHERE ns = {ns:String}`,
            query_params: { ns: this.namespace },
          })
        }
        await migrate.call(this)
      },
      async migrateReset(this: ClickHouseAdapter) {
        // Delete all data for the current namespace
        if (this.clickhouse) {
          await this.clickhouse.command({
            query: `DELETE FROM ${this.table} WHERE ns = {ns:String}`,
            query_params: { ns: this.namespace },
          })
        }
      },
      migrateStatus: async () => {},
      migrationDir: path.resolve(process.cwd(), 'src/migrations'),
    })
  }

  return {
    name: 'clickhouse',
    defaultIDType: 'text',
    init: adapter,
  }
}
