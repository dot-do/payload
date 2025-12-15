import { commitTransaction, initTransaction, killTransaction, readMigrationFiles } from 'payload'

import type { ClickHouseAdapter } from './types.js'

/**
 * Run all pending migrations
 */
export async function migrate(this: ClickHouseAdapter): Promise<void> {
  const { payload } = this

  const migrationFiles = await readMigrationFiles({ payload })

  if (!migrationFiles.length) {
    payload.logger.info({ msg: 'No migrations to run.' })
    return
  }

  // Get already-run migrations from payload-migrations collection
  let existingMigrations: string[] = []
  try {
    const result = await payload.find({
      collection: 'payload-migrations',
      limit: 0,
      pagination: false,
    })
    existingMigrations = result.docs.map((doc) =>
      String((doc as Record<string, unknown>).name || ''),
    )
  } catch {
    // Collection might not exist yet, that's okay
  }

  const pendingMigrations = migrationFiles.filter(
    (migration) => !existingMigrations.includes(migration.name),
  )

  if (!pendingMigrations.length) {
    payload.logger.info({ msg: 'No pending migrations.' })
    return
  }

  const req = { payload }

  // Get the current batch number
  let currentBatch = 1
  try {
    const lastMigration = await payload.find({
      collection: 'payload-migrations',
      limit: 1,
      sort: '-batch',
    })
    if (lastMigration.docs.length > 0) {
      currentBatch = (((lastMigration.docs[0] as Record<string, unknown>).batch as number) || 0) + 1
    }
  } catch {
    // Collection might not exist yet
  }

  for (const migration of pendingMigrations) {
    payload.logger.info({ msg: `Migrating: ${migration.name}` })

    try {
      const start = Date.now()
      await initTransaction(req)

      await migration.up({ payload, req })

      await payload.create({
        collection: 'payload-migrations',
        data: {
          name: migration.name,
          batch: currentBatch,
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
