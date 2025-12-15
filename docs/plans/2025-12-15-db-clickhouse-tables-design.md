# db-clickhouse: Events, Actions, and Search Tables Design

**Date:** 2025-12-15
**Status:** Approved

## Overview

Add three new tables to the db-clickhouse adapter:

1. **events** - Multi-purpose event store for analytics, audit trails, and streaming
2. **actions** - Transaction staging table for ACID-like semantics
3. **search** - Full-text and vector similarity search

## Table Schemas

### Events Table

Multi-purpose event store optimized for analytics, audit trails, and real-time streaming.

```sql
CREATE TABLE events (
  id String,                    -- ULID (time-ordered, sortable)
  ns String,                    -- namespace (tenant isolation)
  timestamp DateTime64(3),      -- when event occurred
  type String,                  -- 'api.request', 'doc.create', 'auth.login', etc.

  -- Common indexed fields
  collection String,            -- affected collection (nullable)
  docId String,                 -- affected document (nullable)
  userId String,                -- who triggered it (nullable)
  sessionId String,             -- session tracking (nullable)
  ip String,                    -- client IP (nullable)
  duration UInt32,              -- milliseconds (for request timing)

  -- Flexible JSON with columnar storage per path
  input JSON,                   -- request body, params, headers
  result JSON                   -- response data, errors, affected count
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (ns, timestamp, type)
```

**Design decisions:**

- ULID for id: time-ordered, lexicographically sortable, better compression
- JSON type: ClickHouse's new JSON stores each path as separate sub-column (columnar performance with JSON flexibility)
- Monthly partitioning: simple TTL management, good for time-range queries
- Separate `input` and `result` fields: capture both request and response

### Actions Table (Transactions)

Staging table for transaction support using full document snapshots.

```sql
CREATE TABLE actions (
  -- Transaction metadata
  txId String,                  -- transaction identifier
  txStatus Enum('pending', 'committed', 'aborted'),
  txTimeout DateTime64(3) NULL, -- NULL = no expiry, otherwise auto-abort after
  txCreatedAt DateTime64(3),    -- when transaction started

  -- Mirror of data table structure
  id String,
  ns String,
  type String,                  -- collection slug or '_globals'
  v DateTime64(3),              -- version timestamp
  data JSON,
  title String,
  createdAt DateTime64(3),
  createdBy String NULL,
  updatedAt DateTime64(3),
  updatedBy String NULL,
  deletedAt DateTime64(3) NULL,
  deletedBy String NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(txCreatedAt)
ORDER BY (ns, txId, type, id)
```

**Transaction flow:**

1. `beginTransaction({ timeout? })` - generates txId, default timeout 30s (null for long-running jobs)
2. All CRUD operations write to `actions` with `txStatus = 'pending'`
3. `commitTransaction(txId)` - copies rows to `data` table, sets `txStatus = 'committed'`
4. `rollbackTransaction(txId)` - sets `txStatus = 'aborted'`

**Design decisions:**

- Full document snapshots (not deltas): simpler, ClickHouse compression handles redundancy (95-98%)
- Optional timeout: null for AI/batch/agentic jobs that may run for hours
- Staging pattern: writes are cheap in ClickHouse, commit is simple INSERT SELECT

### Search Table

Combined full-text and vector similarity search with async embedding generation.

```sql
CREATE TABLE search (
  id String,                    -- ULID
  ns String,                    -- namespace

  -- Document reference
  collection String,            -- source collection
  docId String,                 -- source document id
  chunkIndex UInt16 DEFAULT 0,  -- 0 for single-chunk docs, >0 for chunks

  -- Search content
  text String,                  -- YAML dump of document (for full-text)
  embedding Array(Float32),     -- vector embedding (dimensions from config)

  -- Status for async embedding
  status Enum('pending', 'ready', 'failed'),
  errorMessage String NULL,

  -- Timestamps
  createdAt DateTime64(3),
  updatedAt DateTime64(3),

  -- Full-text index
  INDEX text_idx text TYPE full_text GRANULARITY 1,

  -- Vector similarity index
  INDEX vec_idx embedding TYPE vector_similarity('hnsw', 'cosineDistance')
)
ENGINE = ReplacingMergeTree(updatedAt)
PARTITION BY ns
ORDER BY (ns, collection, docId, chunkIndex)
```

**Sync flow:**

1. Document created/updated - hook inserts to `search` with `status = 'pending'`, `text = yamlDump(doc.data)`
2. External worker queries `WHERE status = 'pending'`, generates embeddings via configured provider
3. Worker updates with `status = 'ready'` and `embedding` array
4. Queries combine full-text (`hasToken`) and vector (`cosineDistance`) for hybrid search

**Design decisions:**

- Single table for both search types: enables hybrid queries without joins
- YAML dump for text: reasonable representation of structured CMS data
- Async embedding: keeps adapter database-focused, allows batching and rate limiting
- Chunking only when needed: most CMS documents fit in one chunk
- Embedding dimensions configurable at adapter init (default 1536 for OpenAI compatibility)

## Adapter API

### New Config Options

```typescript
clickhouseAdapter({
  url: '...',
  embeddingDimensions: 1536, // vector index dimensions (default: 1536)
  defaultTransactionTimeout: 30_000, // ms, null for no timeout (default: 30000)
})
```

### New Methods on `payload.db`

```typescript
// Events
payload.db.logEvent({
  type: string,
  collection?: string,
  docId?: string,
  input?: Record<string, unknown>,
  result?: Record<string, unknown>,
  userId?: string,
  sessionId?: string,
  ip?: string,
  duration?: number,
})

payload.db.queryEvents({
  where?: Where,
  limit?: number,
  page?: number,
  sort?: string,  // default: '-timestamp'
})

// Transactions (replaces current no-op stubs)
payload.db.beginTransaction({ timeout?: number | null })  // returns txId
payload.db.commitTransaction(txId: string)
payload.db.rollbackTransaction(txId: string)

// Search
payload.db.syncToSearch({
  collection: string,
  doc: Document,
  chunkIndex?: number,
})

payload.db.search({
  text?: string,           // full-text query
  vector?: number[],       // embedding vector for similarity
  where?: Where,           // filter by collection, etc.
  limit?: number,
  hybrid?: {
    textWeight: number,
    vectorWeight: number,
  }
})

payload.db.getSearchQueue({ limit?: number })  // for external worker
payload.db.updateSearchStatus(id: string, status: 'ready' | 'failed', embedding?: number[], error?: string)
```

## Table Naming

Simple, unprefixed names:

- `data` - documents (existing)
- `events` - event log
- `actions` - transaction staging
- `search` - full-text and vector search

## Requirements

- ClickHouse 25.8+ for vector similarity indexes
- If older version detected, skip vector index creation (full-text only)

## YAGNI Decisions

Things explicitly deferred:

- Advanced chunking strategies (semantic, field-level) - experiment later
- Auto-partitioning by type/namespace - monthly time partitions sufficient for now
- Embedded embedding generation - external worker keeps adapter simple
- Delta/patch storage for transactions - full snapshots with compression
