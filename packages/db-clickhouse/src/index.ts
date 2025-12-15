import type { DatabaseAdapterObj, Payload } from 'payload'

import { createDatabaseAdapter } from 'payload'

import type { ClickHouseAdapter, ClickHouseAdapterArgs } from './types.js'

import { connect } from './connect.js'
import { destroy } from './destroy.js'
import { init } from './init.js'
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

      // Migration stubs (no migrations needed - schemaless)
      // ClickHouse uses a schemaless JSON approach, so migrations are not needed
      createMigration: async () => {},
      migrate: async () => {},
      async migrateFresh(this: ClickHouseAdapter) {
        // Delete all data for the current namespace only (lightweight delete)
        if (this.clickhouse) {
          await this.clickhouse.command({
            query: `DELETE FROM ${this.table} WHERE ns = {ns:String}`,
            query_params: { ns: this.namespace },
          })
        }
      },
      async migrateRefresh(this: ClickHouseAdapter) {
        // Same as migrateFresh for ClickHouse
        if (this.clickhouse) {
          await this.clickhouse.command({
            query: `DELETE FROM ${this.table} WHERE ns = {ns:String}`,
            query_params: { ns: this.namespace },
          })
        }
      },
      async migrateReset(this: ClickHouseAdapter) {
        // Same as migrateFresh for ClickHouse
        if (this.clickhouse) {
          await this.clickhouse.command({
            query: `DELETE FROM ${this.table} WHERE ns = {ns:String}`,
            query_params: { ns: this.namespace },
          })
        }
      },
      migrateStatus: async () => {},
      migrationDir: '',
    })
  }

  return {
    name: 'clickhouse',
    defaultIDType: 'text',
    init: adapter,
  }
}
