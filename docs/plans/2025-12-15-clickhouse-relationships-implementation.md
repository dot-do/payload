# ClickHouse Relationships Table Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a relationships index table to optimize Join field (reverse lookup) queries.

**Architecture:** Create a `{table}_relationships` table using ReplacingMergeTree, extract relationships on create/update, soft-delete on document delete. Use JOINs for reverse lookups instead of JSON scanning.

**Tech Stack:** ClickHouse, TypeScript, ReplacingMergeTree engine

---

## Task 1: Add RelationshipRow Type

**Files:**

- Modify: `packages/db-clickhouse/src/types.ts`

**Step 1: Add the RelationshipRow interface**

Add after line 138 (after `DataRow` interface):

```typescript
export interface RelationshipRow {
  ns: string
  fromType: string
  fromId: string
  fromField: string
  toType: string
  toId: string
  position: number
  locale: string | null
  v: number // timestamp in milliseconds
  deletedAt: number | null
}
```

**Step 2: Commit**

```bash
git add packages/db-clickhouse/src/types.ts
git commit -m "feat(db-clickhouse): add RelationshipRow type"
```

---

## Task 2: Add Relationships Table Creation

**Files:**

- Modify: `packages/db-clickhouse/src/connect.ts`

**Step 1: Add the SQL generator function**

Add after `getCreateTableSQL` function (around line 53):

```typescript
/**
 * Generate SQL to create the relationships table if it doesn't exist
 */
function getCreateRelationshipsTableSQL(tableName: string): string {
  validateTableName(tableName)
  return `
CREATE TABLE IF NOT EXISTS ${tableName}_relationships (
    ns String,
    fromType String,
    fromId String,
    fromField String,
    toType String,
    toId String,
    position UInt16 DEFAULT 0,
    locale Nullable(String),
    v DateTime64(3, 'UTC'),
    deletedAt Nullable(DateTime64(3, 'UTC'))
) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, toType, toId, fromType, fromId, fromField, position)
`
}
```

**Step 2: Call the function in connect()**

Add after line 122 (after data table creation):

```typescript
// Create the relationships table if it doesn't exist
await this.clickhouse.command({
  query: getCreateRelationshipsTableSQL(this.table),
})
```

**Step 3: Update PAYLOAD_DROP_DATABASE to clear relationships**

Modify the drop database section (around line 127) to also clear relationships:

```typescript
// Delete data for current namespace if PAYLOAD_DROP_DATABASE is set (for tests)
if (process.env.PAYLOAD_DROP_DATABASE === 'true') {
  this.payload.logger.info(
    `---- DROPPING DATA FOR NAMESPACE ${this.namespace} ----`,
  )
  await this.clickhouse.command({
    query: `DELETE FROM ${this.table} WHERE ns = {ns:String}`,
    query_params: { ns: this.namespace },
  })
  await this.clickhouse.command({
    query: `DELETE FROM ${this.table}_relationships WHERE ns = {ns:String}`,
    query_params: { ns: this.namespace },
  })
  this.payload.logger.info('---- DROPPED NAMESPACE DATA ----')
}
```

**Step 4: Commit**

```bash
git add packages/db-clickhouse/src/connect.ts
git commit -m "feat(db-clickhouse): add relationships table creation"
```

---

## Task 3: Create Relationship Extraction Utility

**Files:**

- Create: `packages/db-clickhouse/src/utilities/relationships.ts`

**Step 1: Create the relationships utility file**

```typescript
import type { Field } from 'payload'

import { fieldAffectsData } from 'payload/shared'

import type { RelationshipRow } from '../types.js'

export interface ExtractRelationshipsOptions {
  fromId: string
  fromType: string
  locale?: string
  ns: string
  v: number
}

/**
 * Extract relationships from document data based on field definitions
 */
export function extractRelationships(
  data: Record<string, unknown>,
  fields: Field[],
  options: ExtractRelationshipsOptions,
): RelationshipRow[] {
  const relationships: RelationshipRow[] = []

  for (const field of fields) {
    if (!fieldAffectsData(field)) {
      continue
    }

    if (field.type === 'relationship' || field.type === 'upload') {
      const value = data[field.name]
      if (value === null || value === undefined) {
        continue
      }

      const relationTos = Array.isArray(field.relationTo)
        ? field.relationTo
        : [field.relationTo]

      if (field.hasMany && Array.isArray(value)) {
        value.forEach((item, position) => {
          const rel = parseRelationshipValue(
            item,
            relationTos,
            field.name,
            position,
            options,
          )
          if (rel) {
            relationships.push(rel)
          }
        })
      } else {
        const rel = parseRelationshipValue(
          value,
          relationTos,
          field.name,
          0,
          options,
        )
        if (rel) {
          relationships.push(rel)
        }
      }
    }

    // Recurse into group fields
    if (field.type === 'group' && field.fields) {
      const groupData = data[field.name] as Record<string, unknown> | undefined
      if (groupData) {
        const nested = extractRelationships(groupData, field.fields, options)
        relationships.push(...nested)
      }
    }

    // Recurse into array fields
    if (field.type === 'array' && field.fields) {
      const arrayData = data[field.name] as
        | Record<string, unknown>[]
        | undefined
      if (Array.isArray(arrayData)) {
        for (const item of arrayData) {
          const nested = extractRelationships(item, field.fields, options)
          relationships.push(...nested)
        }
      }
    }

    // Recurse into blocks
    if (field.type === 'blocks' && field.blocks) {
      const blocksData = data[field.name] as
        | Array<{ blockType: string } & Record<string, unknown>>
        | undefined
      if (Array.isArray(blocksData)) {
        for (const block of blocksData) {
          const blockConfig = field.blocks.find(
            (b) => b.slug === block.blockType,
          )
          if (blockConfig?.fields) {
            const nested = extractRelationships(
              block,
              blockConfig.fields,
              options,
            )
            relationships.push(...nested)
          }
        }
      }
    }

    // Recurse into tabs
    if (field.type === 'tabs' && field.tabs) {
      for (const tab of field.tabs) {
        if ('fields' in tab && tab.fields) {
          // Named tab - data is nested under tab name
          if ('name' in tab && tab.name) {
            const tabData = data[tab.name] as
              | Record<string, unknown>
              | undefined
            if (tabData) {
              const nested = extractRelationships(tabData, tab.fields, options)
              relationships.push(...nested)
            }
          } else {
            // Unnamed tab - fields are at root level
            const nested = extractRelationships(data, tab.fields, options)
            relationships.push(...nested)
          }
        }
      }
    }
  }

  return relationships
}

/**
 * Parse a relationship value (handles both simple IDs and polymorphic { relationTo, value })
 */
function parseRelationshipValue(
  value: unknown,
  relationTos: string[],
  fromField: string,
  position: number,
  options: ExtractRelationshipsOptions,
): RelationshipRow | null {
  if (value === null || value === undefined) {
    return null
  }

  let toType: string
  let toId: string

  // Polymorphic relationship: { relationTo: 'collection', value: 'id' }
  if (
    typeof value === 'object' &&
    value !== null &&
    'relationTo' in value &&
    'value' in value
  ) {
    const polymorphic = value as { relationTo: string; value: unknown }
    toType = polymorphic.relationTo
    toId = String(polymorphic.value)
  } else {
    // Simple relationship: just the ID
    toType = relationTos[0]!
    toId = String(value)
  }

  // Skip if toId is empty or invalid
  if (!toId || toId === 'undefined' || toId === 'null') {
    return null
  }

  return {
    ...options,
    fromField,
    toType,
    toId,
    position,
    locale: options.locale ?? null,
    deletedAt: null,
  }
}
```

**Step 2: Commit**

```bash
git add packages/db-clickhouse/src/utilities/relationships.ts
git commit -m "feat(db-clickhouse): add relationship extraction utility"
```

---

## Task 4: Add insertRelationships Helper

**Files:**

- Modify: `packages/db-clickhouse/src/utilities/relationships.ts`

**Step 1: Add the insert function**

Add at the end of the file:

```typescript
import type { ClickHouseClient } from '@clickhouse/client-web'

/**
 * Insert relationship rows into the relationships table
 */
export async function insertRelationships(
  clickhouse: ClickHouseClient,
  table: string,
  relationships: RelationshipRow[],
): Promise<void> {
  if (relationships.length === 0) {
    return
  }

  // Build batch insert
  const values = relationships.map((rel) => ({
    ns: rel.ns,
    fromType: rel.fromType,
    fromId: rel.fromId,
    fromField: rel.fromField,
    toType: rel.toType,
    toId: rel.toId,
    position: rel.position,
    locale: rel.locale,
    v: rel.v,
    deletedAt: rel.deletedAt,
  }))

  await clickhouse.insert({
    table: `${table}_relationships`,
    values,
    format: 'JSONEachRow',
  })
}

/**
 * Soft-delete all relationships for a document
 */
export async function softDeleteRelationships(
  clickhouse: ClickHouseClient,
  table: string,
  options: { ns: string; fromType: string; fromId: string; v: number },
): Promise<void> {
  // Insert tombstone rows for all existing relationships
  // The ReplacingMergeTree will keep only the latest version per key
  const { ns, fromType, fromId, v } = options

  // Query existing relationships to create tombstones
  const result = await clickhouse.query({
    query: `
      SELECT fromField, toType, toId, position, locale
      FROM (
        SELECT *,
          row_number() OVER (
            PARTITION BY ns, fromType, fromId, fromField, toType, toId, position
            ORDER BY v DESC
          ) as _rn
        FROM ${table}_relationships
        WHERE ns = {ns:String} AND fromType = {fromType:String} AND fromId = {fromId:String}
      )
      WHERE _rn = 1 AND deletedAt IS NULL
    `,
    query_params: { ns, fromType, fromId },
    format: 'JSONEachRow',
  })

  const existing = await result.json<{
    fromField: string
    toType: string
    toId: string
    position: number
    locale: string | null
  }>()

  if (existing.length === 0) {
    return
  }

  // Insert tombstones
  const tombstones = existing.map((rel) => ({
    ns,
    fromType,
    fromId,
    fromField: rel.fromField,
    toType: rel.toType,
    toId: rel.toId,
    position: rel.position,
    locale: rel.locale,
    v,
    deletedAt: v,
  }))

  await clickhouse.insert({
    table: `${table}_relationships`,
    values: tombstones,
    format: 'JSONEachRow',
  })
}
```

**Step 2: Update imports at top of file**

```typescript
import type { ClickHouseClient } from '@clickhouse/client-web'
import type { Field } from 'payload'

import { fieldAffectsData } from 'payload/shared'

import type { RelationshipRow } from '../types.js'
```

**Step 3: Commit**

```bash
git add packages/db-clickhouse/src/utilities/relationships.ts
git commit -m "feat(db-clickhouse): add relationship insert/delete helpers"
```

---

## Task 5: Integrate with create.ts

**Files:**

- Modify: `packages/db-clickhouse/src/operations/create.ts`

**Step 1: Add import**

Add after existing imports:

```typescript
import {
  extractRelationships,
  insertRelationships,
} from '../utilities/relationships.js'
```

**Step 2: Extract and insert relationships after document insert**

Add after the `await this.clickhouse.command(...)` call (around line 89):

```typescript
// Extract and insert relationships
const relationships = extractRelationships(docData, collection.config.fields, {
  ns: this.namespace,
  fromType: collectionSlug,
  fromId: id,
  v: now,
})

if (relationships.length > 0) {
  await insertRelationships(this.clickhouse, this.table, relationships)
}
```

**Step 3: Commit**

```bash
git add packages/db-clickhouse/src/operations/create.ts
git commit -m "feat(db-clickhouse): sync relationships on create"
```

---

## Task 6: Integrate with updateOne.ts

**Files:**

- Modify: `packages/db-clickhouse/src/operations/updateOne.ts`

**Step 1: Add import**

Add after existing imports:

```typescript
import {
  extractRelationships,
  insertRelationships,
} from '../utilities/relationships.js'
```

**Step 2: Extract and insert relationships after document update**

Add after the insert command (around line 144):

```typescript
// Extract and insert relationships (overwrites previous with same v)
const relationships = extractRelationships(
  mergedData,
  collection.config.fields,
  {
    ns: this.namespace,
    fromType: collectionSlug,
    fromId: existing.id,
    v: now,
  },
)

if (relationships.length > 0) {
  await insertRelationships(this.clickhouse, this.table, relationships)
}
```

**Step 3: Commit**

```bash
git add packages/db-clickhouse/src/operations/updateOne.ts
git commit -m "feat(db-clickhouse): sync relationships on updateOne"
```

---

## Task 7: Integrate with updateMany.ts

**Files:**

- Modify: `packages/db-clickhouse/src/operations/updateMany.ts`

**Step 1: Add import**

Add after existing imports:

```typescript
import {
  extractRelationships,
  insertRelationships,
} from '../utilities/relationships.js'
```

**Step 2: Collect and batch insert relationships**

Inside the `operations.map()` callback, after building the insert operation, collect relationships. Then batch insert after the main inserts.

Replace the section starting around line 72 with:

```typescript
// Build all insert operations and collect relationships
const allRelationships: RelationshipRow[] = []

const operations = existingRows.map((row) => {
  const existing = parseDataRow(row)
  const existingData = existing.data
  const mergedData = deepMerge(existingData, updateData)
  const title = extractTitle(mergedData, titleField, existing.id)

  const createdAtMs = parseDateTime64ToMs(existing.createdAt)

  const insertParams: QueryParams = {
    id: existing.id,
    type: existing.type,
    createdAtMs,
    data: JSON.stringify(mergedData),
    ns: existing.ns,
    title,
    updatedAt: now,
    v: now,
  }

  if (existing.createdBy) {
    insertParams.createdBy = existing.createdBy
  }
  if (userId !== null) {
    insertParams.updatedBy = userId
  }

  // Extract relationships for this document
  const rels = extractRelationships(mergedData, collection.config.fields, {
    ns: this.namespace,
    fromType: collectionSlug,
    fromId: existing.id,
    v: now,
  })
  allRelationships.push(...rels)

  const insertQuery = `
      INSERT INTO ${this.table} (ns, type, id, v, title, data, createdAt, createdBy, updatedAt, updatedBy, deletedAt, deletedBy)
      VALUES (
        {ns:String},
        {type:String},
        {id:String},
        fromUnixTimestamp64Milli({v:Int64}),
        {title:String},
        {data:String},
        fromUnixTimestamp64Milli({createdAtMs:Int64}),
        ${existing.createdBy ? '{createdBy:String}' : 'NULL'},
        fromUnixTimestamp64Milli({updatedAt:Int64}),
        ${userId !== null ? '{updatedBy:String}' : 'NULL'},
        NULL,
        NULL
      )
    `

  return {
    doc: {
      id: existing.id,
      ...mergedData,
      createdAt: toISOString(existing.createdAt),
      updatedAt: new Date(now).toISOString(),
    } as Document,
    params: insertParams,
    query: insertQuery,
  }
})

// Execute all inserts in parallel
await Promise.all(
  operations.map((op) =>
    this.clickhouse!.command({
      query: op.query,
      query_params: op.params,
    }),
  ),
)

// Batch insert all relationships
if (allRelationships.length > 0) {
  await insertRelationships(this.clickhouse, this.table, allRelationships)
}

return operations.map((op) => op.doc)
```

**Step 3: Add RelationshipRow import**

```typescript
import type { RelationshipRow } from '../types.js'
```

**Step 4: Commit**

```bash
git add packages/db-clickhouse/src/operations/updateMany.ts
git commit -m "feat(db-clickhouse): sync relationships on updateMany"
```

---

## Task 8: Integrate with deleteOne.ts

**Files:**

- Modify: `packages/db-clickhouse/src/operations/deleteOne.ts`

**Step 1: Add import**

Add after existing imports:

```typescript
import { softDeleteRelationships } from '../utilities/relationships.js'
```

**Step 2: Soft-delete relationships after document soft-delete**

Add after the delete command (around line 107):

```typescript
// Soft-delete relationships
await softDeleteRelationships(this.clickhouse, this.table, {
  ns: this.namespace,
  fromType: collectionSlug,
  fromId: existing.id,
  v: deleteV,
})
```

**Step 3: Commit**

```bash
git add packages/db-clickhouse/src/operations/deleteOne.ts
git commit -m "feat(db-clickhouse): soft-delete relationships on deleteOne"
```

---

## Task 9: Integrate with deleteMany.ts

**Files:**

- Modify: `packages/db-clickhouse/src/operations/deleteMany.ts`

**Step 1: Add import**

Add after existing imports:

```typescript
import { softDeleteRelationships } from '../utilities/relationships.js'
```

**Step 2: Soft-delete relationships for each document**

Add after the parallel soft-delete inserts (around line 114):

```typescript
// Soft-delete relationships for all documents
await Promise.all(
  existingRows.map((row) => {
    const existing = parseDataRow(row)
    const existingV = row.v ? parseDateTime64ToMs(row.v) : 0
    const deleteV = Math.max(now, existingV + 1)
    return softDeleteRelationships(this.clickhouse!, this.table, {
      ns: this.namespace,
      fromType: collectionSlug,
      fromId: existing.id,
      v: deleteV,
    })
  }),
)
```

**Step 3: Commit**

```bash
git add packages/db-clickhouse/src/operations/deleteMany.ts
git commit -m "feat(db-clickhouse): soft-delete relationships on deleteMany"
```

---

## Task 10: Build and Test

**Step 1: Build the package**

```bash
cd packages/db-clickhouse && pnpm run build
```

Expected: Build succeeds with no errors

**Step 2: Run integration tests**

```bash
PAYLOAD_DATABASE=clickhouse pnpm run test:int relationships
```

Expected: Tests should pass (or we identify issues to fix)

**Step 3: Final commit**

```bash
git add -A
git commit -m "feat(db-clickhouse): complete relationships table implementation"
```

---

## Future Tasks (Not in Scope)

These are documented for future implementation:

1. **Join field query optimization** - Use relationships table in find operations when Join fields are requested
2. **findJoin operation** - Add dedicated operation for Join field queries using the relationships table
3. **Relationship-aware depth population** - Optimize depth > 0 queries using JOINs
