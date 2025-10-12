# @payloadcms/db-do-sqlite

Durable Objects SQLite database adapter for Payload CMS.

This adapter enables Payload CMS to use Cloudflare Durable Objects SQLite as a database backend, providing persistent, strongly consistent storage with automatic replication.

## Features

- ✅ Full Payload CMS compatibility
- ✅ Drizzle ORM integration with type-safe queries
- ✅ Automatic schema migrations via Drizzle Kit
- ✅ Strongly consistent transactions using Durable Objects
- ✅ Hot reload support in development
- ✅ Production-ready migration system
- ✅ Compatible with all Payload collection features

## Installation

```bash
pnpm add @payloadcms/db-do-sqlite
```

## Basic Usage

### Durable Object Configuration

First, define a Durable Object class that initializes the Payload adapter:

```typescript
import { DurableObject } from 'cloudflare:workers'
import { sqliteDOAdapter } from '@payloadcms/db-do-sqlite'
import { buildConfig } from 'payload'

export class PayloadDO extends DurableObject {
  private payload: Payload | null = null

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
  }

  async fetch(request: Request): Promise<Response> {
    // Initialize Payload on first request
    if (!this.payload) {
      const config = buildConfig({
        db: sqliteDOAdapter({
          storage: this.ctx.storage,
          ctx: this.ctx,
        }),
        collections: [
          // Your collections
        ],
      })

      this.payload = await getPayload({ config })
    }

    // Handle requests with Payload
    return this.payload.handler(request)
  }
}
```

### Worker Configuration

Register the Durable Object in your worker:

```typescript
// src/index.ts
export { PayloadDO } from './durable-object'

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Get Durable Object stub
    const id = env.PAYLOAD_DO.idFromName('payload-instance')
    const stub = env.PAYLOAD_DO.get(id)

    // Forward request to Durable Object
    return stub.fetch(request)
  },
}
```

### wrangler.toml Configuration

```toml
name = "my-payload-app"
main = "src/index.ts"

[[durable_objects.bindings]]
name = "PAYLOAD_DO"
class_name = "PayloadDO"
script_name = "my-payload-app"

[[migrations]]
tag = "v1"
new_classes = ["PayloadDO"]
```

## Configuration Options

The `sqliteDOAdapter` accepts the following options:

```typescript
type Args = {
  // Required: Durable Object storage instance
  storage: DurableObjectStorage

  // Required: Durable Object state/context
  ctx: DurableObjectState

  // ID type for collections (default: 'number')
  idType?: 'integer' | 'numeric' | 'text' | 'uuid'

  // Allow setting ID on create (default: false)
  allowIDOnCreate?: boolean

  // Enable auto-increment IDs (default: false)
  autoIncrement?: boolean

  // Custom locales suffix (default: '_locales')
  localesSuffix?: string

  // Custom relationships suffix (default: '_rels')
  relationshipsSuffix?: string

  // Custom versions suffix (default: '_v')
  versionsSuffix?: string

  // Migration directory (default: './migrations')
  migrationDir?: string

  // Production migrations array
  prodMigrations?: Array<{
    name: string
    up: (args: MigrateUpArgs) => Promise<void>
    down: (args: MigrateDownArgs) => Promise<void>
  }>

  // Enable dev schema push (default: true in development)
  push?: boolean

  // Drizzle logger (default: false)
  logger?: DrizzleConfig['logger']

  // Schema hooks
  beforeSchemaInit?: SQLiteSchemaHook[]
  afterSchemaInit?: SQLiteSchemaHook[]

  // Transaction options
  transactionOptions?: SQLiteTransactionConfig

  // Experimental: Read replicas support
  readReplicas?: 'first-primary'
}
```

## Migrations

### Development Mode

In development, the adapter automatically pushes schema changes to the database:

```bash
# Set environment variable
PAYLOAD_DROP_DATABASE=true pnpm dev
```

This will:
1. Drop existing database on start (if `PAYLOAD_DROP_DATABASE=true`)
2. Push current schema to database
3. Enable hot reload for schema changes

### Production Mode

For production, use Drizzle Kit to generate migration files:

```bash
# Generate migration
pnpm payload migrate:create

# This creates a timestamped migration file in ./migrations/
```

Then import and use migrations in your adapter:

```typescript
import * as migrations from './migrations'

const config = buildConfig({
  db: sqliteDOAdapter({
    storage: ctx.storage,
    ctx: ctx,
    prodMigrations: Object.values(migrations),
  }),
})
```

### Migration Atomicity

All migrations execute within `ctx.blockConcurrencyWhile()` to ensure atomicity:

- Schema changes are atomic (no partial migrations)
- Other requests wait until migration completes
- Strongly consistent reads after migration

## Type-Safe Database Access

Access the underlying Drizzle instance for custom queries:

```typescript
import { sql } from '@payloadcms/db-do-sqlite'

// Inside a Durable Object method
const db = this.payload.db.drizzle

const results = await db
  .select()
  .from(schema.posts)
  .where(sql`published = true`)
```

## Transactions

Transactions are automatically managed by Durable Objects for strong consistency:

```typescript
await payload.create({
  collection: 'posts',
  data: {
    title: 'My Post',
    author: authorId,
  },
  // All operations in this request are atomic
})
```

## Comparison to D1 Adapter

This adapter is architecturally identical to `@payloadcms/db-d1-sqlite`, with key differences:

| Feature | D1 Adapter | DO Adapter |
|---------|------------|------------|
| **Binding** | `binding: AnyD1Database` | `storage: DurableObjectStorage`, `ctx: DurableObjectState` |
| **Consistency** | Eventual (global) | Strong (per-object) |
| **Transactions** | Per-region | Per-Durable Object |
| **Replication** | Automatic global | Automatic within region |
| **Migrations** | Drizzle Kit | Drizzle Kit (with `blockConcurrencyWhile`) |
| **Concurrency** | High (eventually consistent) | Serialized per object |

Choose DO adapter when you need:
- Strong consistency guarantees
- Transactional integrity
- Per-tenant isolation

Choose D1 adapter when you need:
- Global low-latency reads
- Higher concurrent throughput
- Simpler deployment

## Advanced Usage

### Custom Schema Hooks

Extend or modify the generated schema:

```typescript
sqliteDOAdapter({
  storage: ctx.storage,
  ctx: ctx,
  beforeSchemaInit: [
    ({ schema, extendTable }) => {
      // Extend a collection table
      schema.tables.posts = extendTable(schema.tables.posts, {
        viewCount: integer('view_count').default(0),
      })

      return schema
    },
  ],
})
```

### Experimental: Read Replicas

Enable read replica support (experimental):

```typescript
sqliteDOAdapter({
  storage: ctx.storage,
  ctx: ctx,
  readReplicas: 'first-primary',
})
```

## Development

### Building

```bash
pnpm build        # Build with SWC and generate types
pnpm build:swc    # Compile TypeScript with SWC
pnpm build:types  # Generate type definitions
```

### Testing

This adapter is designed to pass the full Payload test suite. See `test/database` in the Payload repository.

## Contributing

This adapter is maintained as part of the `.do` platform and intended for contribution back to the Payload CMS project. Before contributing:

1. Ensure all Payload test suites pass
2. Verify Drizzle ORM compatibility
3. Test migration atomicity with concurrent requests
4. Document any Durable Objects-specific behaviors

## License

MIT

## Resources

- [Payload CMS Documentation](https://payloadcms.com/docs)
- [Drizzle ORM Documentation](https://orm.drizzle.team)
- [Cloudflare Durable Objects](https://developers.cloudflare.com/durable-objects)
- [Drizzle + Durable Objects](https://orm.drizzle.team/docs/connect-cloudflare-do)

## Support

For issues specific to this adapter, please open an issue in the `.do` platform repository. For general Payload questions, visit the [Payload Discord](https://discord.gg/payload).
