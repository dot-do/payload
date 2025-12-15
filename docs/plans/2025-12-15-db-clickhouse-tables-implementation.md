# db-clickhouse: Events, Actions, Search Tables Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add events, actions, and search tables to db-clickhouse adapter with full API support.

**Architecture:** Three new tables created in connect.ts, with new operations for each. Events uses ULID for time-ordered IDs. Actions implements staging-table transactions. Search supports full-text and vector similarity with async embedding.

**Tech Stack:** TypeScript, ClickHouse, ulid package, vitest for unit tests

---

## Task 1: Add ULID Generation Utility

**Files:**

- Modify: `packages/db-clickhouse/src/utilities/generateId.ts`
- Test: `packages/db-clickhouse/src/__tests__/generateId.spec.ts`

**Step 1: Install ulid package**

Run: `cd packages/db-clickhouse && pnpm add ulid`
Expected: Package added to package.json

**Step 2: Add generateUlid function to generateId.ts**

Add after the existing `generateVersion` function:

```typescript
import { ulid } from 'ulid'

/**
 * Generate a ULID (Universally Unique Lexicographically Sortable Identifier)
 * Time-ordered and sortable, ideal for event logs
 */
export function generateUlid(): string {
  return ulid()
}
```

**Step 3: Add test for generateUlid**

Create `packages/db-clickhouse/src/__tests__/generateId.spec.ts`:

```typescript
import { describe, expect, it } from 'vitest'

import {
  generateId,
  generateUlid,
  generateVersion,
} from '../utilities/generateId.js'

describe('generateId', () => {
  it('should generate nanoid by default', () => {
    const id = generateId()
    expect(id).toHaveLength(21)
  })

  it('should generate uuid when specified', () => {
    const id = generateId('uuid')
    expect(id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
    )
  })
})

describe('generateUlid', () => {
  it('should generate 26-character ULID', () => {
    const id = generateUlid()
    expect(id).toHaveLength(26)
  })

  it('should be lexicographically sortable by time', () => {
    const ids: string[] = []
    for (let i = 0; i < 10; i++) {
      ids.push(generateUlid())
    }
    const sorted = [...ids].sort()
    expect(sorted).toEqual(ids)
  })
})

describe('generateVersion', () => {
  it('should generate monotonically increasing versions', () => {
    const v1 = generateVersion()
    const v2 = generateVersion()
    const v3 = generateVersion()
    expect(v2).toBeGreaterThan(v1)
    expect(v3).toBeGreaterThan(v2)
  })
})
```

**Step 4: Run tests**

Run: `cd packages/db-clickhouse && pnpm vitest run src/__tests__/generateId.spec.ts`
Expected: All tests pass

**Step 5: Commit**

```bash
git add packages/db-clickhouse/package.json packages/db-clickhouse/pnpm-lock.yaml
git add packages/db-clickhouse/src/utilities/generateId.ts
git add packages/db-clickhouse/src/__tests__/generateId.spec.ts
git commit -m "feat(db-clickhouse): add ULID generation for time-ordered event IDs"
```

---

## Task 2: Add Events Table Schema

**Files:**

- Modify: `packages/db-clickhouse/src/connect.ts`

**Step 1: Add getCreateEventsTableSQL function**

Add after `getCreateRelationshipsTableSQL`:

```typescript
/**
 * Generate SQL to create the events table if it doesn't exist
 * Events use ULID for time-ordered IDs and JSON for flexible input/result storage
 */
function getCreateEventsTableSQL(): string {
  return `
CREATE TABLE IF NOT EXISTS events (
    id String,
    ns String,
    timestamp DateTime64(3),
    type String,
    collection Nullable(String),
    docId Nullable(String),
    userId Nullable(String),
    sessionId Nullable(String),
    ip Nullable(String),
    duration UInt32 DEFAULT 0,
    input JSON,
    result JSON
) ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (ns, timestamp, type)
`
}
```

**Step 2: Add table creation to connect function**

Add after the relationships table creation (around line 148):

```typescript
// Create the events table if it doesn't exist
await this.clickhouse.command({
  query: getCreateEventsTableSQL(),
})
```

**Step 3: Add DROP for tests**

Add to the PAYLOAD_DROP_DATABASE block:

```typescript
await this.clickhouse.command({
  query: `DELETE FROM events WHERE ns = {ns:String}`,
  query_params: { ns: this.namespace },
})
```

**Step 4: Commit**

```bash
git add packages/db-clickhouse/src/connect.ts
git commit -m "feat(db-clickhouse): add events table schema"
```

---

## Task 3: Add Actions Table Schema

**Files:**

- Modify: `packages/db-clickhouse/src/connect.ts`

**Step 1: Add getCreateActionsTableSQL function**

Add after `getCreateEventsTableSQL`:

```typescript
/**
 * Generate SQL to create the actions table if it doesn't exist
 * Actions table is used for transaction staging - writes go here first,
 * then get copied to data table on commit
 */
function getCreateActionsTableSQL(): string {
  return `
CREATE TABLE IF NOT EXISTS actions (
    txId String,
    txStatus Enum8('pending' = 0, 'committed' = 1, 'aborted' = 2),
    txTimeout Nullable(DateTime64(3)),
    txCreatedAt DateTime64(3),
    id String,
    ns String,
    type String,
    v DateTime64(3),
    data JSON,
    title String DEFAULT '',
    createdAt DateTime64(3),
    createdBy Nullable(String),
    updatedAt DateTime64(3),
    updatedBy Nullable(String),
    deletedAt Nullable(DateTime64(3)),
    deletedBy Nullable(String)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(txCreatedAt)
ORDER BY (ns, txId, type, id)
`
}
```

**Step 2: Add table creation to connect function**

```typescript
// Create the actions table if it doesn't exist
await this.clickhouse.command({
  query: getCreateActionsTableSQL(),
})
```

**Step 3: Add DROP for tests**

```typescript
await this.clickhouse.command({
  query: `DELETE FROM actions WHERE ns = {ns:String}`,
  query_params: { ns: this.namespace },
})
```

**Step 4: Commit**

```bash
git add packages/db-clickhouse/src/connect.ts
git commit -m "feat(db-clickhouse): add actions table schema for transactions"
```

---

## Task 4: Add Search Table Schema

**Files:**

- Modify: `packages/db-clickhouse/src/connect.ts`
- Modify: `packages/db-clickhouse/src/types.ts`

**Step 1: Add embeddingDimensions to config types**

In `types.ts`, add to `ClickHouseAdapterArgs`:

```typescript
export interface ClickHouseAdapterArgs {
  /** Database name (default: 'default') */
  database?: string
  /** Default transaction timeout in ms (default: 30000, null for no timeout) */
  defaultTransactionTimeout?: number | null
  /** Embedding dimensions for vector search (default: 1536) */
  embeddingDimensions?: number
  /** ID type for documents (default: 'text' - nanoid) */
  idType?: 'text' | 'uuid'
  // ... rest unchanged
}
```

And add to `ClickHouseAdapter`:

```typescript
export type ClickHouseAdapter = {
  // ... existing fields
  /** Default transaction timeout in ms */
  defaultTransactionTimeout: number | null
  /** Embedding dimensions for vector search */
  embeddingDimensions: number
  // ... rest unchanged
}
```

**Step 2: Add getCreateSearchTableSQL function to connect.ts**

```typescript
/**
 * Generate SQL to create the search table if it doesn't exist
 * Search table supports both full-text and vector similarity search
 */
function getCreateSearchTableSQL(embeddingDimensions: number): string {
  return `
CREATE TABLE IF NOT EXISTS search (
    id String,
    ns String,
    collection String,
    docId String,
    chunkIndex UInt16 DEFAULT 0,
    text String,
    embedding Array(Float32),
    status Enum8('pending' = 0, 'ready' = 1, 'failed' = 2),
    errorMessage Nullable(String),
    createdAt DateTime64(3),
    updatedAt DateTime64(3),
    INDEX text_idx text TYPE full_text GRANULARITY 1,
    INDEX vec_idx embedding TYPE vector_similarity('hnsw', 'cosineDistance')
) ENGINE = ReplacingMergeTree(updatedAt)
PARTITION BY ns
ORDER BY (ns, collection, docId, chunkIndex)
`
}
```

**Step 3: Update adapter initialization in index.ts**

In `packages/db-clickhouse/src/index.ts`, add the new config:

```typescript
const {
  database = 'default',
  defaultTransactionTimeout = 30_000,
  embeddingDimensions = 1536,
  idType = 'text',
  // ... rest
} = args

// In the adapter function:
return createDatabaseAdapter<ClickHouseAdapter>({
  // ... existing fields
  defaultTransactionTimeout,
  embeddingDimensions,
  // ... rest
})
```

**Step 4: Add table creation to connect function**

```typescript
// Create the search table if it doesn't exist
await this.clickhouse.command({
  query: getCreateSearchTableSQL(this.embeddingDimensions),
})
```

**Step 5: Add DROP for tests**

```typescript
await this.clickhouse.command({
  query: `DELETE FROM search WHERE ns = {ns:String}`,
  query_params: { ns: this.namespace },
})
```

**Step 6: Commit**

```bash
git add packages/db-clickhouse/src/connect.ts
git add packages/db-clickhouse/src/types.ts
git add packages/db-clickhouse/src/index.ts
git commit -m "feat(db-clickhouse): add search table schema with vector index"
```

---

## Task 5: Implement logEvent Operation

**Files:**

- Create: `packages/db-clickhouse/src/operations/logEvent.ts`
- Modify: `packages/db-clickhouse/src/operations/index.ts`
- Modify: `packages/db-clickhouse/src/types.ts`

**Step 1: Add LogEventArgs type to types.ts**

```typescript
export interface LogEventArgs {
  collection?: string
  docId?: string
  duration?: number
  input?: Record<string, unknown>
  ip?: string
  result?: Record<string, unknown>
  sessionId?: string
  type: string
  userId?: string
}
```

**Step 2: Create logEvent.ts**

```typescript
import type { ClickHouseAdapter, LogEventArgs } from '../types.js'

import { generateUlid } from '../utilities/generateId.js'

export async function logEvent(
  this: ClickHouseAdapter,
  args: LogEventArgs,
): Promise<string> {
  const {
    collection,
    docId,
    duration = 0,
    input = {},
    ip,
    result = {},
    sessionId,
    type,
    userId,
  } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const id = generateUlid()
  const now = Date.now()

  const query = `
    INSERT INTO events (id, ns, timestamp, type, collection, docId, userId, sessionId, ip, duration, input, result)
    VALUES (
      {id:String},
      {ns:String},
      fromUnixTimestamp64Milli({timestamp:Int64}),
      {type:String},
      ${collection ? '{collection:String}' : 'NULL'},
      ${docId ? '{docId:String}' : 'NULL'},
      ${userId ? '{userId:String}' : 'NULL'},
      ${sessionId ? '{sessionId:String}' : 'NULL'},
      ${ip ? '{ip:String}' : 'NULL'},
      {duration:UInt32},
      {input:String},
      {result:String}
    )
  `

  const params: Record<string, unknown> = {
    id,
    ns: this.namespace,
    timestamp: now,
    type,
    duration,
    input: JSON.stringify(input),
    result: JSON.stringify(result),
  }

  if (collection) params.collection = collection
  if (docId) params.docId = docId
  if (userId) params.userId = userId
  if (sessionId) params.sessionId = sessionId
  if (ip) params.ip = ip

  await this.clickhouse.command({
    query,
    query_params: params,
  })

  return id
}
```

**Step 3: Export from operations/index.ts**

Add: `export { logEvent } from './logEvent.js'`

**Step 4: Add to adapter in index.ts**

Import and add `logEvent` to the adapter object.

**Step 5: Commit**

```bash
git add packages/db-clickhouse/src/operations/logEvent.ts
git add packages/db-clickhouse/src/operations/index.ts
git add packages/db-clickhouse/src/types.ts
git add packages/db-clickhouse/src/index.ts
git commit -m "feat(db-clickhouse): implement logEvent operation"
```

---

## Task 6: Implement queryEvents Operation

**Files:**

- Create: `packages/db-clickhouse/src/operations/queryEvents.ts`
- Modify: `packages/db-clickhouse/src/operations/index.ts`
- Modify: `packages/db-clickhouse/src/types.ts`

**Step 1: Add QueryEventsArgs and EventRow types to types.ts**

```typescript
export interface QueryEventsArgs {
  limit?: number
  page?: number
  sort?: string
  where?: Where
}

export interface EventRow {
  collection: null | string
  docId: null | string
  duration: number
  id: string
  input: Record<string, unknown>
  ip: null | string
  ns: string
  result: Record<string, unknown>
  sessionId: null | string
  timestamp: string
  type: string
  userId: null | string
}

export interface QueryEventsResult {
  docs: EventRow[]
  hasNextPage: boolean
  hasPrevPage: boolean
  limit: number
  nextPage: null | number
  page: number
  prevPage: null | number
  totalDocs: number
  totalPages: number
}
```

**Step 2: Create queryEvents.ts**

```typescript
import type {
  ClickHouseAdapter,
  EventRow,
  QueryEventsArgs,
  QueryEventsResult,
} from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'

export async function queryEvents(
  this: ClickHouseAdapter,
  args: QueryEventsArgs = {},
): Promise<QueryEventsResult> {
  const { limit = 10, page = 1, sort = '-timestamp', where } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)

  let whereClause = `ns = ${nsParam}`
  const accessWhere = qb.buildWhereClause(where)
  if (accessWhere) {
    whereClause = `${whereClause} AND (${accessWhere})`
  }

  // Parse sort field
  const sortDesc = sort.startsWith('-')
  const sortField = sortDesc ? sort.slice(1) : sort
  const orderBy = `ORDER BY ${sortField} ${sortDesc ? 'DESC' : 'ASC'}`

  const offset = (page - 1) * limit
  const params = qb.getParams()

  // Count query
  const countQuery = `SELECT count() as total FROM events WHERE ${whereClause}`
  const countResult = await this.clickhouse.query({
    format: 'JSONEachRow',
    query: countQuery,
    query_params: params,
  })
  const countRows = await countResult.json<{ total: string }>()
  const totalDocs = parseInt(countRows[0]?.total || '0', 10)

  // Data query
  const dataQuery = `
    SELECT *
    FROM events
    WHERE ${whereClause}
    ${orderBy}
    LIMIT ${limit}
    OFFSET ${offset}
  `

  const dataResult = await this.clickhouse.query({
    format: 'JSONEachRow',
    query: dataQuery,
    query_params: params,
  })

  const rows = await dataResult.json<EventRow>()

  // Parse JSON fields
  const docs = rows.map((row) => ({
    ...row,
    input: typeof row.input === 'string' ? JSON.parse(row.input) : row.input,
    result:
      typeof row.result === 'string' ? JSON.parse(row.result) : row.result,
  }))

  const totalPages = Math.ceil(totalDocs / limit)

  return {
    docs,
    hasNextPage: page < totalPages,
    hasPrevPage: page > 1,
    limit,
    nextPage: page < totalPages ? page + 1 : null,
    page,
    prevPage: page > 1 ? page - 1 : null,
    totalDocs,
    totalPages,
  }
}
```

**Step 3: Export and add to adapter**

**Step 4: Commit**

```bash
git add packages/db-clickhouse/src/operations/queryEvents.ts
git add packages/db-clickhouse/src/operations/index.ts
git add packages/db-clickhouse/src/types.ts
git add packages/db-clickhouse/src/index.ts
git commit -m "feat(db-clickhouse): implement queryEvents operation"
```

---

## Task 7: Implement Transaction Operations

**Files:**

- Create: `packages/db-clickhouse/src/operations/beginTransaction.ts`
- Create: `packages/db-clickhouse/src/operations/commitTransaction.ts`
- Create: `packages/db-clickhouse/src/operations/rollbackTransaction.ts`
- Modify: `packages/db-clickhouse/src/operations/index.ts`
- Modify: `packages/db-clickhouse/src/types.ts`

**Step 1: Add BeginTransactionArgs type to types.ts**

```typescript
export interface BeginTransactionArgs {
  /** Timeout in ms. Use null for no timeout (long-running jobs). */
  timeout?: number | null
}
```

**Step 2: Create beginTransaction.ts**

```typescript
import type { ClickHouseAdapter, BeginTransactionArgs } from '../types.js'

import { generateId } from '../utilities/generateId.js'

export async function beginTransaction(
  this: ClickHouseAdapter,
  args: BeginTransactionArgs = {},
): Promise<string> {
  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const { timeout = this.defaultTransactionTimeout } = args
  const txId = generateId(this.idType)
  const now = Date.now()

  const timeoutValue = timeout !== null ? now + timeout : null

  const query = `
    INSERT INTO actions (txId, txStatus, txTimeout, txCreatedAt, id, ns, type, v, data, title, createdAt, updatedAt)
    VALUES (
      {txId:String},
      'pending',
      ${timeoutValue !== null ? 'fromUnixTimestamp64Milli({txTimeout:Int64})' : 'NULL'},
      fromUnixTimestamp64Milli({txCreatedAt:Int64}),
      '',
      {ns:String},
      '_tx_metadata',
      fromUnixTimestamp64Milli({v:Int64}),
      '{}',
      '',
      fromUnixTimestamp64Milli({createdAt:Int64}),
      fromUnixTimestamp64Milli({updatedAt:Int64})
    )
  `

  const params: Record<string, unknown> = {
    txId,
    txCreatedAt: now,
    ns: this.namespace,
    v: now,
    createdAt: now,
    updatedAt: now,
  }

  if (timeoutValue !== null) {
    params.txTimeout = timeoutValue
  }

  await this.clickhouse.command({
    query,
    query_params: params,
  })

  return txId
}
```

**Step 3: Create commitTransaction.ts**

```typescript
import type { ClickHouseAdapter } from '../types.js'

import { generateVersion } from '../utilities/generateId.js'

export async function commitTransaction(
  this: ClickHouseAdapter,
  txId: string,
): Promise<void> {
  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const now = generateVersion()

  // Copy all pending actions for this transaction to the data table
  const copyQuery = `
    INSERT INTO ${this.table} (ns, type, id, v, title, data, createdAt, createdBy, updatedAt, updatedBy, deletedAt, deletedBy)
    SELECT ns, type, id, v, title, data, createdAt, createdBy, updatedAt, updatedBy, deletedAt, deletedBy
    FROM actions
    WHERE txId = {txId:String}
      AND txStatus = 'pending'
      AND ns = {ns:String}
      AND type != '_tx_metadata'
  `

  await this.clickhouse.command({
    query: copyQuery,
    query_params: { txId, ns: this.namespace },
  })

  // Update transaction status to committed
  // Note: ClickHouse doesn't support UPDATE, so we insert a new row with updated status
  // The ReplacingMergeTree will eventually merge and keep the latest
  const updateQuery = `
    INSERT INTO actions (txId, txStatus, txTimeout, txCreatedAt, id, ns, type, v, data, title, createdAt, updatedAt)
    SELECT
      txId,
      'committed' as txStatus,
      txTimeout,
      txCreatedAt,
      id,
      ns,
      type,
      fromUnixTimestamp64Milli({v:Int64}) as v,
      data,
      title,
      createdAt,
      fromUnixTimestamp64Milli({updatedAt:Int64}) as updatedAt
    FROM actions
    WHERE txId = {txId:String}
      AND txStatus = 'pending'
      AND ns = {ns:String}
      AND type = '_tx_metadata'
  `

  await this.clickhouse.command({
    query: updateQuery,
    query_params: { txId, ns: this.namespace, v: now, updatedAt: now },
  })
}
```

**Step 4: Create rollbackTransaction.ts**

```typescript
import type { ClickHouseAdapter } from '../types.js'

import { generateVersion } from '../utilities/generateId.js'

export async function rollbackTransaction(
  this: ClickHouseAdapter,
  txId: string,
): Promise<void> {
  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const now = generateVersion()

  // Update transaction status to aborted
  const updateQuery = `
    INSERT INTO actions (txId, txStatus, txTimeout, txCreatedAt, id, ns, type, v, data, title, createdAt, updatedAt)
    SELECT
      txId,
      'aborted' as txStatus,
      txTimeout,
      txCreatedAt,
      id,
      ns,
      type,
      fromUnixTimestamp64Milli({v:Int64}) as v,
      data,
      title,
      createdAt,
      fromUnixTimestamp64Milli({updatedAt:Int64}) as updatedAt
    FROM actions
    WHERE txId = {txId:String}
      AND txStatus = 'pending'
      AND ns = {ns:String}
      AND type = '_tx_metadata'
  `

  await this.clickhouse.command({
    query: updateQuery,
    query_params: { txId, ns: this.namespace, v: now, updatedAt: now },
  })
}
```

**Step 5: Export and update adapter**

Replace the no-op transaction stubs in index.ts with the real implementations.

**Step 6: Commit**

```bash
git add packages/db-clickhouse/src/operations/beginTransaction.ts
git add packages/db-clickhouse/src/operations/commitTransaction.ts
git add packages/db-clickhouse/src/operations/rollbackTransaction.ts
git add packages/db-clickhouse/src/operations/index.ts
git add packages/db-clickhouse/src/index.ts
git commit -m "feat(db-clickhouse): implement real transaction operations"
```

---

## Task 8: Implement syncToSearch Operation

**Files:**

- Create: `packages/db-clickhouse/src/operations/syncToSearch.ts`
- Modify: `packages/db-clickhouse/src/operations/index.ts`
- Modify: `packages/db-clickhouse/src/types.ts`

**Step 1: Add SyncToSearchArgs type to types.ts**

```typescript
export interface SyncToSearchArgs {
  chunkIndex?: number
  collection: string
  doc: Record<string, unknown>
}
```

**Step 2: Create syncToSearch.ts**

```typescript
import type { ClickHouseAdapter, SyncToSearchArgs } from '../types.js'

import { generateUlid, generateVersion } from '../utilities/generateId.js'

/**
 * Convert a document to YAML-like string representation for full-text search
 */
function docToText(doc: Record<string, unknown>, indent = 0): string {
  const lines: string[] = []
  const prefix = '  '.repeat(indent)

  for (const [key, value] of Object.entries(doc)) {
    if (value === null || value === undefined) {
      continue
    }

    if (Array.isArray(value)) {
      lines.push(`${prefix}${key}:`)
      for (const item of value) {
        if (typeof item === 'object' && item !== null) {
          lines.push(`${prefix}  -`)
          lines.push(docToText(item as Record<string, unknown>, indent + 2))
        } else {
          lines.push(`${prefix}  - ${String(item)}`)
        }
      }
    } else if (typeof value === 'object') {
      lines.push(`${prefix}${key}:`)
      lines.push(docToText(value as Record<string, unknown>, indent + 1))
    } else {
      lines.push(`${prefix}${key}: ${String(value)}`)
    }
  }

  return lines.join('\n')
}

export async function syncToSearch(
  this: ClickHouseAdapter,
  args: SyncToSearchArgs,
): Promise<string> {
  const { chunkIndex = 0, collection, doc } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const docId = String(doc.id)
  const id = generateUlid()
  const now = generateVersion()
  const text = docToText(doc)

  // Create empty embedding array with correct dimensions
  const emptyEmbedding = new Array(this.embeddingDimensions).fill(0)

  const query = `
    INSERT INTO search (id, ns, collection, docId, chunkIndex, text, embedding, status, createdAt, updatedAt)
    VALUES (
      {id:String},
      {ns:String},
      {collection:String},
      {docId:String},
      {chunkIndex:UInt16},
      {text:String},
      {embedding:Array(Float32)},
      'pending',
      fromUnixTimestamp64Milli({createdAt:Int64}),
      fromUnixTimestamp64Milli({updatedAt:Int64})
    )
  `

  await this.clickhouse.command({
    query,
    query_params: {
      id,
      ns: this.namespace,
      collection,
      docId,
      chunkIndex,
      text,
      embedding: emptyEmbedding,
      createdAt: now,
      updatedAt: now,
    },
  })

  return id
}
```

**Step 3: Export and add to adapter**

**Step 4: Commit**

```bash
git add packages/db-clickhouse/src/operations/syncToSearch.ts
git add packages/db-clickhouse/src/operations/index.ts
git add packages/db-clickhouse/src/types.ts
git add packages/db-clickhouse/src/index.ts
git commit -m "feat(db-clickhouse): implement syncToSearch operation"
```

---

## Task 9: Implement search Operation

**Files:**

- Create: `packages/db-clickhouse/src/operations/search.ts`
- Modify: `packages/db-clickhouse/src/operations/index.ts`
- Modify: `packages/db-clickhouse/src/types.ts`

**Step 1: Add SearchArgs and SearchResult types to types.ts**

```typescript
export interface SearchArgs {
  hybrid?: {
    textWeight: number
    vectorWeight: number
  }
  limit?: number
  text?: string
  vector?: number[]
  where?: Where
}

export interface SearchResultDoc {
  chunkIndex: number
  collection: string
  docId: string
  id: string
  score: number
  text: string
}

export interface SearchResult {
  docs: SearchResultDoc[]
}
```

**Step 2: Create search.ts**

```typescript
import type {
  ClickHouseAdapter,
  SearchArgs,
  SearchResult,
  SearchResultDoc,
} from '../types.js'

import { QueryBuilder } from '../queries/QueryBuilder.js'

export async function search(
  this: ClickHouseAdapter,
  args: SearchArgs = {},
): Promise<SearchResult> {
  const { hybrid, limit = 10, text, vector, where } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  if (!text && !vector) {
    return { docs: [] }
  }

  const qb = new QueryBuilder()
  const nsParam = qb.addNamedParam('ns', this.namespace)

  let whereClause = `ns = ${nsParam} AND status = 'ready'`
  const accessWhere = qb.buildWhereClause(where)
  if (accessWhere) {
    whereClause = `${whereClause} AND (${accessWhere})`
  }

  const params = qb.getParams()

  let query: string
  let scoreExpr: string

  if (text && vector && hybrid) {
    // Hybrid search: combine text and vector scores
    const textWeight = hybrid.textWeight
    const vectorWeight = hybrid.vectorWeight
    params.searchText = text.toLowerCase()
    params.searchVector = vector

    scoreExpr = `(
      ${textWeight} * (CASE WHEN position(lower(text), {searchText:String}) > 0 THEN 1 ELSE 0 END) +
      ${vectorWeight} * (1 - cosineDistance(embedding, {searchVector:Array(Float32)}))
    ) as score`

    query = `
      SELECT id, collection, docId, chunkIndex, text, ${scoreExpr}
      FROM search
      WHERE ${whereClause}
      ORDER BY score DESC
      LIMIT ${limit}
    `
  } else if (vector) {
    // Vector-only search
    params.searchVector = vector

    query = `
      SELECT id, collection, docId, chunkIndex, text,
        (1 - cosineDistance(embedding, {searchVector:Array(Float32)})) as score
      FROM search
      WHERE ${whereClause}
      ORDER BY cosineDistance(embedding, {searchVector:Array(Float32)})
      LIMIT ${limit}
    `
  } else {
    // Text-only search
    params.searchText = text!.toLowerCase()

    query = `
      SELECT id, collection, docId, chunkIndex, text,
        (CASE WHEN position(lower(text), {searchText:String}) > 0 THEN 1 ELSE 0 END) as score
      FROM search
      WHERE ${whereClause} AND position(lower(text), {searchText:String}) > 0
      ORDER BY position(lower(text), {searchText:String})
      LIMIT ${limit}
    `
  }

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = await result.json<SearchResultDoc>()

  return { docs: rows }
}
```

**Step 3: Export and add to adapter**

**Step 4: Commit**

```bash
git add packages/db-clickhouse/src/operations/search.ts
git add packages/db-clickhouse/src/operations/index.ts
git add packages/db-clickhouse/src/types.ts
git add packages/db-clickhouse/src/index.ts
git commit -m "feat(db-clickhouse): implement search operation with hybrid support"
```

---

## Task 10: Implement Search Queue Operations

**Files:**

- Create: `packages/db-clickhouse/src/operations/getSearchQueue.ts`
- Create: `packages/db-clickhouse/src/operations/updateSearchStatus.ts`
- Modify: `packages/db-clickhouse/src/operations/index.ts`
- Modify: `packages/db-clickhouse/src/types.ts`

**Step 1: Add types to types.ts**

```typescript
export interface GetSearchQueueArgs {
  limit?: number
}

export interface SearchQueueItem {
  collection: string
  docId: string
  id: string
  text: string
}

export interface UpdateSearchStatusArgs {
  embedding?: number[]
  error?: string
  id: string
  status: 'failed' | 'ready'
}
```

**Step 2: Create getSearchQueue.ts**

```typescript
import type {
  ClickHouseAdapter,
  GetSearchQueueArgs,
  SearchQueueItem,
} from '../types.js'

export async function getSearchQueue(
  this: ClickHouseAdapter,
  args: GetSearchQueueArgs = {},
): Promise<SearchQueueItem[]> {
  const { limit = 100 } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const query = `
    SELECT id, collection, docId, text
    FROM search
    WHERE ns = {ns:String} AND status = 'pending'
    ORDER BY createdAt ASC
    LIMIT ${limit}
  `

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params: { ns: this.namespace },
  })

  return result.json<SearchQueueItem>()
}
```

**Step 3: Create updateSearchStatus.ts**

```typescript
import type { ClickHouseAdapter, UpdateSearchStatusArgs } from '../types.js'

import { generateVersion } from '../utilities/generateId.js'

export async function updateSearchStatus(
  this: ClickHouseAdapter,
  args: UpdateSearchStatusArgs,
): Promise<void> {
  const { embedding, error, id, status } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const now = generateVersion()

  // For ReplacingMergeTree, we need to insert a new row with updated values
  // First, get the existing row
  const selectQuery = `
    SELECT id, ns, collection, docId, chunkIndex, text, createdAt
    FROM search
    WHERE id = {id:String} AND ns = {ns:String}
    ORDER BY updatedAt DESC
    LIMIT 1
  `

  const selectResult = await this.clickhouse.query({
    format: 'JSONEachRow',
    query: selectQuery,
    query_params: { id, ns: this.namespace },
  })

  const rows = await selectResult.json<{
    chunkIndex: number
    collection: string
    createdAt: string
    docId: string
    id: string
    ns: string
    text: string
  }>()

  if (rows.length === 0) {
    throw new Error(`Search document with id '${id}' not found`)
  }

  const row = rows[0]!
  const embeddingValue =
    embedding || new Array(this.embeddingDimensions).fill(0)

  const insertQuery = `
    INSERT INTO search (id, ns, collection, docId, chunkIndex, text, embedding, status, errorMessage, createdAt, updatedAt)
    VALUES (
      {id:String},
      {ns:String},
      {collection:String},
      {docId:String},
      {chunkIndex:UInt16},
      {text:String},
      {embedding:Array(Float32)},
      {status:String},
      ${error ? '{errorMessage:String}' : 'NULL'},
      parseDateTimeBestEffort({createdAt:String}),
      fromUnixTimestamp64Milli({updatedAt:Int64})
    )
  `

  const params: Record<string, unknown> = {
    id: row.id,
    ns: row.ns,
    collection: row.collection,
    docId: row.docId,
    chunkIndex: row.chunkIndex,
    text: row.text,
    embedding: embeddingValue,
    status,
    createdAt: row.createdAt,
    updatedAt: now,
  }

  if (error) {
    params.errorMessage = error
  }

  await this.clickhouse.command({
    query: insertQuery,
    query_params: params,
  })
}
```

**Step 4: Export and add to adapter**

**Step 5: Commit**

```bash
git add packages/db-clickhouse/src/operations/getSearchQueue.ts
git add packages/db-clickhouse/src/operations/updateSearchStatus.ts
git add packages/db-clickhouse/src/operations/index.ts
git add packages/db-clickhouse/src/types.ts
git add packages/db-clickhouse/src/index.ts
git commit -m "feat(db-clickhouse): implement search queue operations for async embedding"
```

---

## Task 11: Update Type Exports and Module Augmentation

**Files:**

- Modify: `packages/db-clickhouse/src/types.ts`
- Modify: `packages/db-clickhouse/src/index.ts`

**Step 1: Update module augmentation in types.ts**

Update the `declare module 'payload'` block to include new methods:

```typescript
declare module 'payload' {
  export interface DatabaseAdapter
    extends Omit<ClickHouseAdapterArgs, 'password' | 'url'> {
    clickhouse: ClickHouseClient | null
    defaultTransactionTimeout: number | null
    embeddingDimensions: number
    execute: <T = unknown>(args: ExecuteArgs<T>) => Promise<T[]>
    getSearchQueue: (args?: GetSearchQueueArgs) => Promise<SearchQueueItem[]>
    logEvent: (args: LogEventArgs) => Promise<string>
    queryEvents: (args?: QueryEventsArgs) => Promise<QueryEventsResult>
    search: (args?: SearchArgs) => Promise<SearchResult>
    syncToSearch: (args: SyncToSearchArgs) => Promise<string>
    updateSearchStatus: (args: UpdateSearchStatusArgs) => Promise<void>
    upsertMany: (args: UpsertManyArgs) => Promise<Document[]>
  }
}
```

**Step 2: Export new types from index.ts**

```typescript
export type {
  BeginTransactionArgs,
  ClickHouseAdapter,
  ClickHouseAdapterArgs,
  EventRow,
  ExecuteArgs,
  GetSearchQueueArgs,
  LogEventArgs,
  MigrateDownArgs,
  MigrateUpArgs,
  QueryEventsArgs,
  QueryEventsResult,
  SearchArgs,
  SearchQueueItem,
  SearchResult,
  SearchResultDoc,
  SyncToSearchArgs,
  UpdateSearchStatusArgs,
  UpsertManyArgs,
} from './types.js'
```

**Step 3: Commit**

```bash
git add packages/db-clickhouse/src/types.ts
git add packages/db-clickhouse/src/index.ts
git commit -m "feat(db-clickhouse): update type exports and module augmentation"
```

---

## Task 12: Add Integration Tests

**Files:**

- Create: `packages/db-clickhouse/src/__tests__/events.spec.ts`
- Create: `packages/db-clickhouse/src/__tests__/search.spec.ts`

**Step 1: Create events.spec.ts**

```typescript
import { describe, expect, it } from 'vitest'

describe('events', () => {
  it.todo('should log an event with all fields')
  it.todo('should query events with pagination')
  it.todo('should filter events by type')
  it.todo('should sort events by timestamp')
})
```

**Step 2: Create search.spec.ts**

```typescript
import { describe, expect, it } from 'vitest'

describe('search', () => {
  it.todo('should sync a document to search')
  it.todo('should get pending items from search queue')
  it.todo('should update search status with embedding')
  it.todo('should perform text search')
  it.todo('should perform vector search')
  it.todo('should perform hybrid search')
})
```

**Step 3: Commit**

```bash
git add packages/db-clickhouse/src/__tests__/events.spec.ts
git add packages/db-clickhouse/src/__tests__/search.spec.ts
git commit -m "test(db-clickhouse): add placeholder integration tests for events and search"
```

---

## Task 13: Build and Verify

**Step 1: Run linting**

Run: `pnpm run lint packages/db-clickhouse`
Expected: No errors

**Step 2: Build the package**

Run: `pnpm run build:db-clickhouse`
Expected: Build succeeds

**Step 3: Run unit tests**

Run: `cd packages/db-clickhouse && pnpm vitest run`
Expected: All tests pass

**Step 4: Final commit**

```bash
git add -A
git commit -m "chore(db-clickhouse): lint and build verification"
```

---

## Summary

| Task | Description              | Parallelizable |
| ---- | ------------------------ | -------------- |
| 1    | Add ULID generation      | Yes            |
| 2    | Add events table schema  | After 1        |
| 3    | Add actions table schema | Yes            |
| 4    | Add search table schema  | Yes            |
| 5    | Implement logEvent       | After 1, 2     |
| 6    | Implement queryEvents    | After 5        |
| 7    | Implement transactions   | After 3        |
| 8    | Implement syncToSearch   | After 4        |
| 9    | Implement search         | After 8        |
| 10   | Implement search queue   | After 8        |
| 11   | Update type exports      | After 5-10     |
| 12   | Add integration tests    | After 11       |
| 13   | Build and verify         | After 12       |

**Parallel execution groups:**

- Group A: Tasks 1, 3, 4 (can run in parallel)
- Group B: Tasks 2, 5, 6 (sequential, depends on 1)
- Group C: Task 7 (depends on 3)
- Group D: Tasks 8, 9, 10 (sequential, depends on 4)
- Group E: Tasks 11, 12, 13 (sequential, final)
