import type { DrizzleAdapter } from '@payloadcms/drizzle'
import type { Connect, Migration } from 'payload'

import { pushDevSchema } from '@payloadcms/drizzle'
import { drizzle } from 'drizzle-orm/durable-sqlite'

import type { DOClickHouseAdapter } from './types.js'

import { createClickHouseClient } from './clickhouse/client.js'
import { initOplogTable } from './durable-object/oplog.js'

export const connect: Connect = async function connect(
  this: DOClickHouseAdapter,
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

    // Initialize ClickHouse client
    try {
      this.clickhouse = await createClickHouseClient(this.clickhouseConfig)
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('[db-do-clickhouse] Failed to connect to ClickHouse:', err)
      // Continue without ClickHouse - local operations will still work
      this.clickhouse = null
    }

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
    this.payload.logger.error({
      err,
      msg: `Error: cannot connect to Durable Object SQLite: ${message}`,
    })
    if (typeof this.rejectInitializing === 'function') {
      this.rejectInitializing()
    }
    // eslint-disable-next-line no-console
    console.error(err)
    process.exit(1)
  }

  // Only push schema if not in production
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

  // Run production migrations atomically
  if (process.env.NODE_ENV === 'production' && this.prodMigrations) {
    await this.ctx.blockConcurrencyWhile(async () => {
      await this.migrate({ migrations: this.prodMigrations as Migration[] })
    })
  }

  // Initialize oplog table after schema push/migrations
  // This ensures the table exists even after dropDatabase
  initOplogTable(this.drizzle)

  if (typeof this.resolveInitializing === 'function') {
    this.resolveInitializing()
  }
}
