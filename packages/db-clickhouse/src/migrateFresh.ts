import { commitTransaction, initTransaction, killTransaction, readMigrationFiles } from 'payload'

import type { ClickHouseAdapter } from './types.js'

/**
 * Drop all data for the current namespace and run all migrations from scratch
 */
export async function migrateFresh(
  this: ClickHouseAdapter,
  { forceAcceptWarning = false }: { forceAcceptWarning?: boolean } = {},
): Promise<void> {
  const { payload } = this

  if (!forceAcceptWarning && typeof process !== 'undefined' && process.stdin?.isTTY) {
    // Only prompt in interactive mode - skip for tests
    payload.logger.warn({
      msg: `WARNING: This will delete all data for namespace '${this.namespace}' and run all migrations.`,
    })
  }

  payload.logger.info({
    msg: `Dropping data for namespace '${this.namespace}'.`,
  })

  // Delete all data for the current namespace
  if (this.clickhouse) {
    await this.clickhouse.command({
      query: `DELETE FROM ${this.table} WHERE ns = {ns:String}`,
      query_params: { ns: this.namespace },
    })
  }

  const migrationFiles = await readMigrationFiles({ payload })
  payload.logger.debug({
    msg: `Found ${migrationFiles.length} migration files.`,
  })

  const req = { payload }

  // Run all migrations
  for (const migration of migrationFiles) {
    payload.logger.info({ msg: `Migrating: ${migration.name}` })
    try {
      const start = Date.now()
      await initTransaction(req)

      await migration.up({ payload, req })

      await payload.create({
        collection: 'payload-migrations',
        data: {
          name: migration.name,
          batch: 1,
        },
        req,
      })

      await commitTransaction(req)

      payload.logger.info({ msg: `Migrated: ${migration.name} (${Date.now() - start}ms)` })
    } catch (err: unknown) {
      await killTransaction(req)
      payload.logger.error({
        err,
        msg: `Error running migration ${migration.name}. Rolling back.`,
      })
      throw err
    }
  }
}
