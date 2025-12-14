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

## Usage

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

## Configuration Options

| Option      | Type               | Default     | Description                                       |
| ----------- | ------------------ | ----------- | ------------------------------------------------- |
| `url`       | `string`           | _required_  | ClickHouse server URL (e.g., `https://host:8443`) |
| `username`  | `string`           | `'default'` | ClickHouse username                               |
| `password`  | `string`           | `''`        | ClickHouse password                               |
| `database`  | `string`           | `'default'` | Database name                                     |
| `table`     | `string`           | `'data'`    | Table name for storing all documents              |
| `namespace` | `string`           | `'payload'` | Namespace to separate different Payload apps      |
| `idType`    | `'text' \| 'uuid'` | `'text'`    | ID type for documents                             |

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
- **FINAL Modifier**: All queries use `FINAL` to get the latest version, which adds overhead
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

To run ClickHouse locally for development:

```bash
docker run -d \
  --name clickhouse \
  -p 8123:8123 \
  -p 9000:9000 \
  clickhouse/clickhouse-server:latest
```

More detailed usage can be found in the [Payload Docs](https://payloadcms.com/docs/configuration/overview).
