# Payload ClickHouse Adapter

ClickHouse database adapter for [Payload](https://payloadcms.com), optimized for Cloudflare Workers and edge deployments.

- [Main Repository](https://github.com/payloadcms/payload)
- [Payload Docs](https://payloadcms.com/docs)

> **⚠️ EXPERIMENTAL**: This adapter is experimental and designed for specialized use cases. ClickHouse is an OLAP (Online Analytical Processing) database optimized for analytics workloads, not traditional CMS operations. See [OLAP Limitations](#olap-limitations) for important considerations before using this in production.

## Features

- **Cloudflare Workers Compatible**: Uses `@clickhouse/client-web` (HTTP-based) for edge runtime compatibility
- **Single Table Design**: All data stored in one table with namespace separation
- **Schemaless**: No migrations needed - data stored as JSON
- **Soft Deletes**: Documents are never hard deleted, uses `deletedAt` timestamp
- **Version Tracking**: Uses `v` (DateTime64) field for versioning with ReplacingMergeTree

## Installation

```bash
npm install @dotdo/db-clickhouse
```

For local development with embedded ClickHouse (chdb), also install the optional `chdb` peer dependency:

```bash
npm install @dotdo/db-clickhouse chdb
```

> **Note**: `chdb` is an optional peer dependency (~85MB) that's only needed if you use the `chdbAdapter`. The main `clickhouseAdapter` works without it.

## Usage

### Remote ClickHouse Server

```ts
import { buildConfig } from 'payload'
import { clickhouseAdapter } from '@dotdo/db-clickhouse'

export default buildConfig({
  db: clickhouseAdapter({
    url: process.env.CLICKHOUSE_URL,
    username: process.env.CLICKHOUSE_USERNAME,
    password: process.env.CLICKHOUSE_PASSWORD,
    database: 'myapp',
    namespace: 'production',
  }),
  // ...rest of config
})
```

### Embedded ClickHouse with chdb

For local development, you can use the embedded ClickHouse adapter powered by [chdb](https://clickhouse.com/docs/chdb). This runs ClickHouse embedded in your Node.js process with file-based persistence - no Docker or external server required.

```ts
import { buildConfig } from 'payload'
import { chdbAdapter } from '@dotdo/db-clickhouse/local'

export default buildConfig({
  db: chdbAdapter({
    path: './.data/clickhouse', // Directory for data storage
    namespace: 'development',
  }),
  // ...rest of config
})
```

The chdb adapter is ideal for:

- Local development without Docker
- Quick prototyping and testing
- CI/CD pipelines
- Environments where running a ClickHouse server isn't practical

> **Note**: Requires the optional `chdb` peer dependency. Data is persisted to the specified directory and survives restarts.

### Bring Your Own Client

You can pass a pre-configured ClickHouse client instead of connection credentials. This is useful for advanced configurations, connection pooling, or using a custom client wrapper.

```ts
import { buildConfig } from 'payload'
import { createClient } from '@clickhouse/client-web'
import { clickhouseAdapter } from '@dotdo/db-clickhouse'

// Create your own client with custom configuration
const client = createClient({
  url: 'https://your-clickhouse-host:8443',
  username: 'default',
  password: process.env.CLICKHOUSE_PASSWORD,
  clickhouse_settings: {
    allow_experimental_json_type: 1,
    // your custom settings...
  },
})

export default buildConfig({
  db: clickhouseAdapter({
    client, // Pass the pre-configured client
    database: 'myapp',
    namespace: 'production',
  }),
  // ...rest of config
})
```

You can also pass a chdb Session directly:

```ts
import { buildConfig } from 'payload'
import { Session } from 'chdb'
import { clickhouseAdapter } from '@dotdo/db-clickhouse'

const session = new Session('/path/to/data')

export default buildConfig({
  db: clickhouseAdapter({ session }),
  // ...rest of config
})
```

For custom clients, implement the `ClickHouseClientLike` interface:

```ts
interface ClickHouseClientLike {
  close(): Promise<void>
  command(params: { query: string; query_params?: Record<string, unknown> }): Promise<unknown>
  query<T = unknown>(params: {
    format?: string
    query: string
    query_params?: Record<string, unknown>
  }): Promise<{ json: <R = T>() => Promise<R[]> }>
}
```

## Configuration Options

### Remote Adapter (`clickhouseAdapter`)

| Option                      | Type                   | Default      | Description                                                              |
| --------------------------- | ---------------------- | ------------ | ------------------------------------------------------------------------ |
| `client`                    | `ClickHouseClientLike` | `undefined`  | Pre-configured client (if provided, `url`/`username`/`password` ignored) |
| `session`                   | `ChdbSession`          | `undefined`  | chdb Session instance (automatically wrapped)                            |
| `url`                       | `string`               | _required\*_ | ClickHouse server URL (e.g., `https://host:8443`)                        |
| `username`                  | `string`               | `'default'`  | ClickHouse username                                                      |
| `password`                  | `string`               | `''`         | ClickHouse password                                                      |
| `database`                  | `string`               | `'default'`  | Database name                                                            |
| `table`                     | `string`               | `'data'`     | Table name for storing all documents                                     |
| `namespace`                 | `string`               | `'payload'`  | Namespace to separate different Payload apps (supports dots for domains) |
| `idType`                    | `'text' \| 'uuid'`     | `'text'`     | ID type for documents                                                    |
| `timezone`                  | `string`               | `'UTC'`      | Timezone for DateTime handling. Use `'auto'` to detect from environment  |
| `embeddingDimensions`       | `number`               | `1536`       | Embedding dimensions for vector search                                   |
| `vectorIndex`               | `VectorIndexConfig`    | `undefined`  | Vector index configuration (see below)                                   |
| `defaultTransactionTimeout` | `number \| null`       | `30000`      | Default transaction timeout in milliseconds                              |

> \*`url` is required only if `client` or `session` is not provided.

### chdb Adapter (`chdbAdapter`)

| Option                      | Type                | Default                | Description                                  |
| --------------------------- | ------------------- | ---------------------- | -------------------------------------------- |
| `path`                      | `string`            | `'./.data/clickhouse'` | Directory for chdb data storage              |
| `namespace`                 | `string`            | `'payload'`            | Namespace to separate different Payload apps |
| `table`                     | `string`            | `'data'`               | Table name for storing all documents         |
| `idType`                    | `'text' \| 'uuid'`  | `'text'`               | ID type for documents                        |
| `embeddingDimensions`       | `number`            | `1536`                 | Embedding dimensions for vector search       |
| `vectorIndex`               | `VectorIndexConfig` | `undefined`            | Vector index configuration (see below)       |
| `defaultTransactionTimeout` | `number \| null`    | `30000`                | Default transaction timeout in milliseconds  |

### Vector Index Configuration

To enable vector similarity search indexing on the search table:

```ts
import { clickhouseAdapter } from '@dotdo/db-clickhouse'

export default buildConfig({
  db: clickhouseAdapter({
    url: process.env.CLICKHOUSE_URL,
    // ...other options
    vectorIndex: {
      enabled: true,
      metric: 'cosineDistance', // or 'L2Distance' (default)
    },
  }),
})
```

| Option    | Type                               | Default        | Description                    |
| --------- | ---------------------------------- | -------------- | ------------------------------ |
| `enabled` | `boolean`                          | `false`        | Enable vector similarity index |
| `metric`  | `'L2Distance' \| 'cosineDistance'` | `'L2Distance'` | Distance metric for similarity |

> **Note**: Vector indexes require ClickHouse's experimental vector similarity features to be enabled on your server.

## Database Schema

The adapter creates a single table with the following structure:

```sql
CREATE TABLE IF NOT EXISTS data (
    ns String,                    -- namespace (separates payload apps)
    type String,                  -- collection slug
    id String,                    -- document ID
    v DateTime64(3),              -- version timestamp
    title String DEFAULT '',      -- useAsTitle field for admin UI
    data JSON,                    -- document data
    createdAt DateTime64(3),
    createdBy Nullable(String),
    updatedAt DateTime64(3),
    updatedBy Nullable(String),
    deletedAt Nullable(DateTime64(3)),
    deletedBy Nullable(String)
) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, type, id, v)
```

## Cloudflare Workers

This adapter is designed to work with Cloudflare Workers. Make sure your ClickHouse instance is accessible via HTTPS and configure your worker with the appropriate bindings.

```ts
// wrangler.toml
;[vars]
CLICKHOUSE_URL = 'https://your-clickhouse-host:8443'
CLICKHOUSE_USERNAME = 'default'
CLICKHOUSE_DATABASE = 'myapp'

// Use secrets for password
// wrangler secret put CLICKHOUSE_PASSWORD
```

## OLAP Limitations

ClickHouse is an OLAP (Online Analytical Processing) database designed for high-throughput analytics queries on large datasets. It has fundamental architectural differences from OLTP databases like PostgreSQL or MongoDB that make it behave differently for CMS workloads:

### No ACID Transactions

ClickHouse does not support ACID transactions. This means:

- **No Rollback**: If an operation fails mid-way, partial data may already be written
- **No Atomic Multi-Document Updates**: Updates to multiple documents are not atomic
- **Transaction Stubs**: The adapter provides `beginTransaction`, `commitTransaction`, and `rollbackTransaction` methods that log warnings but do not actually provide transactional guarantees

### Eventual Consistency

This adapter uses ClickHouse's `ReplacingMergeTree` engine:

- **Background Merges**: Duplicate rows (same id) are deduplicated during background merge operations, not immediately
- **Window Functions**: Queries use window functions (`row_number() OVER PARTITION BY`) to get the latest version per document
- **Read-After-Write**: Immediately reading data after writing may return stale results until merges complete
- **Not Real-Time**: ClickHouse prioritizes batch analytics over real-time consistency

### Performance Characteristics

- **Optimized for**: Large batch reads, analytical queries, aggregations
- **Not Optimized for**: Single-row lookups, frequent small updates, real-time OLTP
- **Write Latency**: Higher than traditional databases due to merge operations
- **Query Latency**: Excellent for large scans, but `FINAL` adds overhead for small queries

### When to Use This Adapter

**Good Use Cases:**

- Content analytics and reporting dashboards
- High-read, low-write content archives
- Edge deployments requiring HTTP-based database access
- Logging and audit trail storage
- Multi-tenant applications requiring namespace separation

**Poor Use Cases:**

- High-frequency content updates
- Applications requiring strong consistency guarantees
- Real-time collaborative editing
- Traditional CRUD-heavy CMS operations

### Soft Deletes

All delete operations are soft deletes (setting `deletedAt` timestamp). Data is never physically removed, which is important for:

- Audit compliance
- Data recovery
- Analytics on historical data

## Local Development

### Option 1: Embedded ClickHouse with chdb (Recommended)

The easiest way to develop locally is using the chdb adapter. No Docker or external services required:

```bash
# Install chdb alongside the adapter
npm install chdb
```

```ts
// payload.config.ts
import { chdbAdapter } from '@dotdo/db-clickhouse/local'

export default buildConfig({
  db: chdbAdapter({
    path: './.data/clickhouse',
    namespace: 'dev',
  }),
})
```

Data is persisted to the specified directory and survives restarts. Add `.data/` to your `.gitignore`.

### Option 2: Docker

Alternatively, run a full ClickHouse server with Docker:

```bash
docker run -d \
  --name clickhouse \
  -p 8123:8123 \
  -p 9000:9000 \
  clickhouse/clickhouse-server:latest
```

Then use the remote adapter:

```ts
import { clickhouseAdapter } from '@dotdo/db-clickhouse'

export default buildConfig({
  db: clickhouseAdapter({
    url: 'http://localhost:8123',
    database: 'default',
    namespace: 'dev',
  }),
})
```

More detailed usage can be found in the [Payload Docs](https://payloadcms.com/docs/configuration/overview).
