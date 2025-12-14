# db-do-clickhouse Design

A hybrid database adapter combining Durable Object SQLite for transactional local storage with ClickHouse for cross-tenant analytics.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Payload CMS                               │
│   payload.find() / payload.create() / payload.db.clickhouse     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    db-do-clickhouse Adapter                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │ getNamespaceId│───▶│  DO Router   │───▶│ Multi-tenant │      │
│  │   (req) → id  │    │              │    │   Plugin     │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
└─────────────────────────────────────────────────────────────────┘
          │                                         │
          ▼                                         ▼
┌─────────────────────┐                 ┌─────────────────────────┐
│   Durable Object    │    sync via     │       ClickHouse        │
│   (per ns/tenant)   │────alarm()─────▶│    (cross-DO queries)   │
│                     │                 │                         │
│  ┌───────────────┐  │                 │  ns | type | id | data  │
│  │    SQLite     │  │                 │  ─────────────────────  │
│  │  (Drizzle)    │  │                 │  prod│posts│abc│{...}   │
│  │  + _oplog     │  │                 │  prod│users│def│{...}   │
│  └───────────────┘  │                 └─────────────────────────┘
└─────────────────────┘
```

## Key Design Decisions

### 1. Operation-based Routing

- Standard CRUD operations (find, create, update, delete) use local SQLite
- Cross-DO analytics/search use ClickHouse via `payload.db.clickhouse`

### 2. Namespace & Tenant Isolation

- DO ID derived from `getNamespaceId(req)` function
- Default: just namespace (e.g., `prod`)
- With multi-tenant plugin: `namespace:tenant` (e.g., `prod:tenant-123`)
- Each DO has isolated SQLite database

### 3. Sync Mechanism

- Oplog table tracks all mutations with `synced` flag
- Alarm scheduled only when mutations occur (not constantly polling)
- Small batch window (100ms) to group rapid mutations
- Exponential backoff on ClickHouse failures
- Version timestamps handle out-of-order delivery

### 4. ClickHouse Schema (Hybrid)

- Core indexed columns: ns, tenant, type, id, timestamps
- Flexible JSON column for all field data
- ReplacingMergeTree for deduplication by version

### 5. Consistency Model

- SQLite: Strong consistency, full ACID transactions
- ClickHouse: Eventual consistency (typically <1s lag)
- Users see their writes immediately (local SQLite)
- Cross-tenant queries may be slightly stale

## Oplog Schema

```sql
CREATE TABLE _oplog (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  op TEXT NOT NULL,           -- 'insert' | 'update' | 'delete'
  collection TEXT NOT NULL,   -- 'posts', 'users', etc.
  doc_id TEXT NOT NULL,       -- document ID
  data JSON,                  -- full document (null for deletes)
  timestamp INTEGER NOT NULL, -- Date.now()
  synced INTEGER DEFAULT 0    -- 0 = pending, 1 = synced
);

CREATE INDEX _oplog_pending ON _oplog(synced, seq);
```

## Sync Flow

```ts
// After any write operation
async afterMutation() {
  const currentAlarm = await this.ctx.storage.getAlarm()
  if (!currentAlarm) {
    await this.ctx.storage.setAlarm(Date.now() + 100) // 100ms batch window
  }
}

// Alarm handler
async alarm() {
  const pending = await this.syncPendingToClickHouse()
  if (pending.hasMore) {
    await this.ctx.storage.setAlarm(Date.now()) // More to sync
  }
}
```

## ClickHouse Schema

```sql
CREATE TABLE data (
    ns String,
    tenant String DEFAULT '',
    type String,
    id String,
    v DateTime64(3),

    createdAt DateTime64(3),
    updatedAt DateTime64(3),
    createdBy Nullable(String),
    updatedBy Nullable(String),
    deletedAt Nullable(DateTime64(3)),

    data JSON,
    title String DEFAULT ''

) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, tenant, type, id)
PARTITION BY (ns, type)
```

Note: Partitioning strategy may need revision if namespace/type count grows large.

## Adapter Configuration

```ts
import { doClickhouseAdapter } from '@payloadcms/db-do-clickhouse'

export default buildConfig({
  db: doClickhouseAdapter({
    clickhouse: {
      url: 'https://your-clickhouse:8443',
      database: 'myapp',
      username: 'default',
      password: '...',
    },

    namespace: 'prod',

    durableObject: {
      binding: 'PAYLOAD_DO',
    },

    getNamespaceId: (req) => {
      const tenant = req.headers.get('x-payload-tenant')
      return tenant ? `prod:${tenant}` : 'prod'
    },

    sync: {
      batchWindow: 100,
      batchSize: 100,
      retentionDays: 7,
    },

    indexes: {
      posts: ['slug', 'category'],
      users: ['email'],
    },
  }),
})
```

## Exposed API

- `payload.db.drizzle` - Local SQLite Drizzle instance
- `payload.db.clickhouse` - ClickHouse client for cross-DO queries
- `payload.db.namespace` - Current namespace
- `payload.db.sync()` - Force immediate sync

## Multi-Tenant Integration

| Scenario                | DO ID           | SQLite Scope  | ClickHouse Query                            |
| ----------------------- | --------------- | ------------- | ------------------------------------------- |
| Single tenant           | `prod`          | All data      | `WHERE ns = 'prod'`                         |
| Multi-tenant (tenant A) | `prod:tenant-a` | Tenant A only | `WHERE ns = 'prod' AND tenant = 'tenant-a'` |
| Cross-tenant analytics  | N/A (direct CH) | N/A           | `WHERE ns = 'prod'`                         |

## Package Structure

```
packages/db-do-clickhouse/
├── src/
│   ├── index.ts
│   ├── types.ts
│   ├── connect.ts
│   ├── destroy.ts
│   │
│   ├── durable-object/
│   │   ├── PayloadDO.ts
│   │   ├── alarm.ts
│   │   └── oplog.ts
│   │
│   ├── operations/
│   │   ├── create.ts
│   │   ├── find.ts
│   │   ├── findOne.ts
│   │   ├── updateOne.ts
│   │   ├── deleteOne.ts
│   │   └── ...
│   │
│   ├── sync/
│   │   ├── syncToClickHouse.ts
│   │   ├── transform.ts
│   │   └── retry.ts
│   │
│   └── clickhouse/
│       ├── client.ts
│       ├── schema.ts
│       └── queries.ts
│
├── package.json
└── tsconfig.json
```

## Key Reuse

- **db-do-sqlite**: Drizzle setup, schema generation, SQLite operations
- **db-clickhouse**: ClickHouse client, query builder, transform utilities
