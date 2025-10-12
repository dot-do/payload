/**
 * Drizzle ORM Durable Objects SQLite Adapter
 *
 * This module re-exports all exports from drizzle-orm's durable-sqlite driver,
 * which provides integration with Cloudflare Durable Objects SQL storage.
 *
 * Key exports:
 * - `drizzle(storage, config)` - Initialize Drizzle with DurableObjectStorage
 * - `DrizzleSqliteDODatabase` - Type for the Drizzle database instance
 * - `SQLiteDOSession` - Session type for Durable Object SQLite
 *
 * Usage:
 * ```ts
 * import { drizzle } from '@payloadcms/db-do-sqlite/drizzle-proxy/durable-sqlite'
 * import { DurableObject } from 'cloudflare:workers'
 *
 * export class MyDO extends DurableObject {
 *   db: DrizzleSqliteDODatabase<typeof schema>
 *
 *   constructor(ctx: DurableObjectState, env: Env) {
 *     super(ctx, env)
 *     this.db = drizzle(ctx.storage, { schema, logger: false })
 *
 *     // Run migrations atomically during initialization
 *     ctx.blockConcurrencyWhile(async () => {
 *       await migrate(this.db, { migrationsFolder: './migrations' })
 *     })
 *   }
 * }
 * ```
 *
 * @see https://orm.drizzle.team/docs/connect-cloudflare-do
 */
export * from 'drizzle-orm/durable-sqlite'
