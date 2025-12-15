import type { DatabaseAdapterObj, Payload } from 'payload'

import path from 'path'
import { createDatabaseAdapter } from 'payload'
import { fileURLToPath } from 'url'

import type { ClickHouseAdapter, ClickHouseAdapterArgs } from './types.js'

import { connect } from './connect.js'
import { createMigration } from './createMigration.js'
import { destroy } from './destroy.js'
import { init } from './init.js'
import { migrate } from './migrate.js'
import { migrateFresh } from './migrateFresh.js'
import {
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
  queryDrafts,
  updateGlobal,
  updateGlobalVersion,
  updateMany,
  updateOne,
  updateVersion,
  upsert,
  upsertMany,
} from './operations/index.js'

export type {
  ClickHouseAdapter,
  ClickHouseAdapterArgs,
  ExecuteArgs,
  MigrateDownArgs,
  MigrateUpArgs,
  UpsertManyArgs,
} from './types.js'

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
    database = 'default',
    idType = 'text',
    namespace = 'payload',
    password = '',
    table = 'data',
    url,
    username = 'default',
  } = args

  function adapter({ payload }: { payload: Payload }) {
    return createDatabaseAdapter<ClickHouseAdapter>({
      name: 'clickhouse',
      defaultIDType: 'text',
      packageName: '@dotdo/db-clickhouse',

      // ClickHouse-specific config
      clickhouse: null, // Set during connect, exposes raw client via payload.db.clickhouse
      config: { database, idType, namespace, password, table, url, username },
      database,
      idType,
      namespace,
      table,

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

      // Raw query execution
      execute,

      // Transaction stubs - ClickHouse is an OLAP database and does not support ACID transactions
      // Operations are eventually consistent via ReplacingMergeTree
      // We implement no-op stubs so Payload operations that use transactions can still work
      beginTransaction: () => Promise.resolve(null),
      commitTransaction: async () => {},
      rollbackTransaction: async () => {},

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
