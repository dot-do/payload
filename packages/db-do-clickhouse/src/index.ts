import type { Operators } from '@payloadcms/drizzle'
import type { DatabaseAdapterObj, Payload, PayloadRequest } from 'payload'

import {
  beginTransaction,
  buildCreateMigration,
  commitTransaction,
  count,
  countGlobalVersions,
  countVersions,
  create,
  createGlobal,
  createGlobalVersion,
  createSchemaGenerator,
  createVersion,
  deleteMany,
  deleteOne,
  deleteVersions,
  find,
  findDistinct,
  findGlobal,
  findGlobalVersions,
  findMigrationDir,
  findOne,
  findVersions,
  migrate,
  migrateDown,
  migrateFresh,
  migrateRefresh,
  migrateReset,
  migrateStatus,
  operatorMap,
  queryDrafts,
  rollbackTransaction,
  updateGlobal,
  updateGlobalVersion,
  updateJobs,
  updateMany,
  updateOne,
  updateVersion,
  upsert,
} from '@payloadcms/drizzle'
import {
  columnToCodeConverter,
  convertPathToJSONTraversal,
  countDistinct,
  createJSONQuery,
  defaultDrizzleSnapshot,
  deleteWhere,
  init,
  insert,
  requireDrizzleKit,
} from '@payloadcms/drizzle/sqlite'
import { like, notLike } from 'drizzle-orm'
import { createDatabaseAdapter, defaultBeginTransaction } from 'payload'
import { fileURLToPath } from 'url'

import type { DOClickHouseAdapter, DOClickHouseAdapterArgs, SyncConfig } from './types.js'

import { connect } from './connect.js'
import { destroy } from './destroy.js'
import { dropDatabase } from './dropDatabase.js'
import { forceSync, scheduleAlarmIfNeeded } from './durable-object/alarm.js'
import { appendToOplog } from './durable-object/oplog.js'
import { execute } from './execute.js'

const filename = fileURLToPath(import.meta.url)

// Default sync configuration
const defaultSyncConfig: Required<SyncConfig> = {
  batchSize: 100,
  batchWindow: 100,
  retentionDays: 7,
}

export function doClickhouseAdapter(
  args: DOClickHouseAdapterArgs,
): DatabaseAdapterObj<DOClickHouseAdapter> {
  const sqliteIDType = args.idType || 'uuid'
  const payloadIDType = sqliteIDType === 'uuid' ? 'text' : 'number'
  const allowIDOnCreate = args.allowIDOnCreate ?? false

  // Merge sync config with defaults
  const syncConfig: Required<SyncConfig> = {
    ...defaultSyncConfig,
    ...args.sync,
  }

  // Default namespace ID function
  const defaultGetNamespaceId = (_req: PayloadRequest) => args.namespace

  function adapter({ payload }: { payload: Payload }) {
    const migrationDir = findMigrationDir(args.migrationDir)
    let resolveInitializing: () => void = () => {}
    let rejectInitializing: () => void = () => {}

    const initializing = new Promise<void>((res, rej) => {
      resolveInitializing = res
      rejectInitializing = rej
    })

    // SQLite's like operator is case-insensitive
    const operators = {
      ...operatorMap,
      contains: like,
      like,
      not_like: notLike,
    } as unknown as Operators

    // Extract tenant from namespace ID (format: "ns" or "ns:tenant")
    const getTenant = (namespaceId: string): string => {
      const parts = namespaceId.split(':')
      return parts.length > 1 ? parts.slice(1).join(':') : ''
    }

    const adapterInstance = createDatabaseAdapter<DOClickHouseAdapter>({
      name: 'sqlite',
      afterSchemaInit: args.afterSchemaInit ?? [],
      allowIDOnCreate,
      autoIncrement: args.autoIncrement ?? false,
      beforeSchemaInit: args.beforeSchemaInit ?? [],
      blocksAsJSON: args.blocksAsJSON ?? false,
      clickhouse: null,
      clickhouseConfig: args.clickhouse,
      ctx: args.ctx,
      storage: args.storage,
      // @ts-expect-error - initialized in connect
      client: undefined,
      defaultDrizzleSnapshot,
      // @ts-expect-error - initialized in connect
      drizzle: undefined,
      features: {
        json: true,
      },
      fieldConstraints: {},
      fieldIndexes: args.fieldIndexes ?? {},
      generateSchema: createSchemaGenerator({
        columnToCodeConverter,
        corePackageSuffix: 'sqlite-core',
        defaultOutputFile: args.generateSchemaOutputFile,
        tableImport: 'sqliteTable',
      }),
      getNamespaceId: args.getNamespaceId ?? defaultGetNamespaceId,
      idType: sqliteIDType,
      initializing,
      limitedBoundParameters: true,
      localesSuffix: args.localesSuffix || '_locales',
      logger: args.logger,
      namespace: args.namespace,
      operators,
      prodMigrations: args.prodMigrations,
      // @ts-expect-error - vestiges of when tsconfig was not strict
      indexes: new Set<string>(),
      push: args.push,
      rawRelations: {},
      rawTables: {},
      readReplicas: args.readReplicas,
      relations: {},
      relationshipsSuffix: args.relationshipsSuffix || '_rels',
      schema: {},
      schemaName: args.schemaName,
      sessions: {},
      syncConfig,
      tableNameMap: new Map<string, string>(),
      tables: {},
      // @ts-expect-error - vestiges of when tsconfig was not strict
      execute,
      // @ts-expect-error - vestiges of when tsconfig was not strict
      transactionOptions: args.transactionOptions || undefined,
      updateJobs,
      versionsSuffix: args.versionsSuffix || '_v',

      // Oplog methods - wrap mutations to track in oplog (synchronous)
      appendToOplog(
        this: DOClickHouseAdapter,
        entry: {
          collection: string
          data: null | Record<string, unknown>
          doc_id: string
          op: 'delete' | 'insert' | 'update'
        },
      ) {
        appendToOplog(this.drizzle, entry)
        // Schedule alarm asynchronously but don't wait for it
        void scheduleAlarmIfNeeded(this.ctx, this.syncConfig.batchWindow)
      },

      // Force sync method
      async sync(this: DOClickHouseAdapter) {
        const namespaceId = this.namespace
        const tenant = getTenant(namespaceId)
        return forceSync(this.ctx, this.drizzle, this.clickhouse, {
          namespace: this.namespace,
          syncConfig: this.syncConfig,
          table: this.clickhouseConfig.table || 'data',
          tenant,
        })
      },

      // DatabaseAdapter methods
      beginTransaction: args.transactionOptions ? beginTransaction : defaultBeginTransaction(),
      commitTransaction,
      connect,
      convertPathToJSONTraversal,
      count,
      countDistinct,
      countGlobalVersions,
      countVersions,
      create: wrapWithOplog(create, 'insert'),
      createGlobal: wrapGlobalWithOplog(createGlobal, 'insert'),
      createGlobalVersion,
      createJSONQuery,
      createMigration: buildCreateMigration({
        executeMethod: 'run',
        filename,
        sanitizeStatements({ sqlExecute, statements }) {
          return statements
            .map((statement) => `${sqlExecute}${statement?.replaceAll('`', '\\`')}\`)`)
            .join('\n')
        },
      }),
      createVersion,
      defaultIDType: payloadIDType,
      deleteMany: wrapWithOplog(deleteMany, 'delete'),
      deleteOne: wrapWithOplog(deleteOne, 'delete'),
      deleteVersions,
      deleteWhere,
      destroy,
      dropDatabase,
      find,
      findDistinct,
      findGlobal,
      findGlobalVersions,
      findOne,
      findVersions,
      init,
      insert,
      migrate,
      migrateDown,
      migrateFresh,
      migrateRefresh,
      migrateReset,
      migrateStatus,
      migrationDir,
      packageName: '@payloadcms/db-do-clickhouse',
      payload,
      queryDrafts,
      rejectInitializing,
      requireDrizzleKit,
      resolveInitializing,
      rollbackTransaction,
      updateGlobal: wrapGlobalWithOplog(updateGlobal, 'update'),
      updateGlobalVersion,
      updateMany: wrapWithOplog(updateMany, 'update'),
      updateOne: wrapWithOplog(updateOne, 'update'),
      updateVersion,
      upsert: wrapWithOplog(upsert, 'update'),
    })

    return adapterInstance
  }

  return {
    name: 'do-clickhouse',
    allowIDOnCreate,
    defaultIDType: payloadIDType,
    init: adapter,
  }
}

/**
 * Wrap a collection operation to append to oplog after execution
 */
function wrapWithOplog<T extends (...args: any[]) => Promise<any>>(
  operation: T,
  op: 'delete' | 'insert' | 'update',
): T {
  return async function (this: DOClickHouseAdapter, args: any) {
    const result = await operation.call(this, args)

    // After successful operation, append to oplog (synchronous)
    if (result && args.collection) {
      const docId = result.id ?? args.id ?? args.where?.id?.equals
      if (docId) {
        this.appendToOplog({
          collection: args.collection,
          data: op === 'delete' ? null : result,
          doc_id: String(docId),
          op,
        })
      }
    }

    return result
  } as T
}

/**
 * Wrap a global operation to append to oplog after execution
 */
function wrapGlobalWithOplog<T extends (...args: any[]) => Promise<any>>(
  operation: T,
  op: 'delete' | 'insert' | 'update',
): T {
  return async function (this: DOClickHouseAdapter, args: any) {
    const result = await operation.call(this, args)

    // After successful operation, append to oplog (synchronous)
    // Globals use slug as the identifier
    if (result && args.slug) {
      this.appendToOplog({
        collection: `_globals`,
        data: op === 'delete' ? null : result,
        doc_id: args.slug,
        op,
      })
    }

    return result
  } as T
}

// Re-export types
export type {
  ClickHouseConfig,
  DOClickHouseAdapter,
  DOClickHouseAdapterArgs,
  IndexConfig,
  SyncConfig,
} from './types.js'

export { sql } from 'drizzle-orm'
