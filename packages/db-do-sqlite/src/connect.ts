import type { DrizzleAdapter } from '@payloadcms/drizzle'
import type { Connect, Migration } from 'payload'

import { pushDevSchema } from '@payloadcms/drizzle'
import { drizzle } from 'drizzle-orm/durable-sqlite'

import type { SQLiteDOAdapter } from './types.js'

export const connect: Connect = async function connect(
  this: SQLiteDOAdapter,
  options = {
    hotReload: false,
  },
) {
  const { hotReload } = options

  this.schema = {
    ...this.tables,
    ...this.relations,
  }

  try {
    const logger = this.logger || false
    const storage = this.storage
    const ctx = this.ctx

    // Initialize Drizzle with Durable Object storage
    this.drizzle = drizzle(storage, {
      logger,
      schema: this.schema,
    })

    // Store reference to underlying storage for compatibility
    this.client = storage as any

    // Run migrations atomically during initialization
    if (!hotReload) {
      if (process.env.PAYLOAD_DROP_DATABASE === 'true') {
        this.payload.logger.info(`---- DROPPING TABLES ----`)

        // Wrap database drop in blockConcurrencyWhile for atomicity
        await ctx.blockConcurrencyWhile(async () => {
          await this.dropDatabase({ adapter: this })
        })

        this.payload.logger.info('---- DROPPED TABLES ----')
      }
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    this.payload.logger.error({ err, msg: `Error: cannot connect to Durable Object SQLite: ${message}` })
    if (typeof this.rejectInitializing === 'function') {
      this.rejectInitializing()
    }
    console.error(err)
    process.exit(1)
  }

  // Only push schema if not in production
  // Schema changes in Durable Objects should be wrapped for atomicity
  if (
    process.env.NODE_ENV !== 'production' &&
    process.env.PAYLOAD_MIGRATING !== 'true' &&
    this.push !== false
  ) {
    // Wrap schema push in blockConcurrencyWhile for atomicity
    await this.ctx.blockConcurrencyWhile(async () => {
      await pushDevSchema(this as unknown as DrizzleAdapter)
    })
  }

  if (typeof this.resolveInitializing === 'function') {
    this.resolveInitializing()
  }

  // Run production migrations atomically
  if (process.env.NODE_ENV === 'production' && this.prodMigrations) {
    await this.ctx.blockConcurrencyWhile(async () => {
      await this.migrate({ migrations: this.prodMigrations as Migration[] })
    })
  }
}
