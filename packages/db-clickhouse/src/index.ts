import type { DatabaseAdapterObj, Payload } from 'payload'

import { createDatabaseAdapter, defaultBeginTransaction } from 'payload'

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
} from './operations/index.js'

export type { ClickHouseAdapter, ClickHouseAdapterArgs } from './types.js'

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
      client: null,
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

      // Upsert
      upsert,

      // Transaction stubs - ClickHouse is an OLAP database and does not support ACID transactions
      // Operations are eventually consistent via ReplacingMergeTree
      beginTransaction: defaultBeginTransaction(),
      commitTransaction: async () => {
        // eslint-disable-next-line no-console
        console.warn(
          '[@dotdo/db-clickhouse] commitTransaction called but ClickHouse does not support transactions. ' +
            'Operations are eventually consistent.',
        )
        await Promise.resolve()
      },
      rollbackTransaction: async () => {
        // eslint-disable-next-line no-console
        console.warn(
          '[@dotdo/db-clickhouse] rollbackTransaction called but ClickHouse does not support transactions. ' +
            'Data may already be written and cannot be rolled back.',
        )
        await Promise.resolve()
      },

      // Migration stubs (no migrations needed - schemaless)
      // ClickHouse uses a schemaless JSON approach, so migrations are not needed
      createMigration: async () => {},
      migrate: async () => {},
      migrateFresh: async () => {},
      migrateRefresh: async () => {},
      migrateReset: async () => {},
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
