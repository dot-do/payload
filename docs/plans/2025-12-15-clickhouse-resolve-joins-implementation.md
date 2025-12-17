# ClickHouse Resolve Joins Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement `resolveJoins()` to populate Join fields using the relationships table for efficient reverse lookups.

**Architecture:** Post-query resolution pattern (like MongoDB). After `find()` returns documents, `resolveJoins()` queries the relationships table to find documents referencing the parent docs, then attaches results with pagination metadata.

**Tech Stack:** ClickHouse SQL with window functions, TypeScript

---

### Task 1: Create resolveJoins utility with types

**Files:**

- Create: `packages/db-clickhouse/src/utilities/resolveJoins.ts`

**Step 1: Create the file with types and function signature**

```typescript
import type { JoinQuery, SanitizedJoins } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

export type ResolveJoinsArgs = {
  adapter: ClickHouseAdapter
  collectionSlug: string
  docs: Record<string, unknown>[]
  joins?: JoinQuery
  locale?: string
  versions?: boolean
}

type SanitizedJoin = SanitizedJoins[string][number]

/**
 * Resolves join relationships for a collection of documents.
 * Queries the relationships table to find documents that reference
 * the parent documents, then attaches them with pagination metadata.
 */
export async function resolveJoins({
  adapter,
  collectionSlug,
  docs,
  joins,
  locale,
  versions = false,
}: ResolveJoinsArgs): Promise<void> {
  // Early return if no joins requested or no documents
  if (!joins || joins === false || docs.length === 0) {
    return
  }

  const collectionConfig = adapter.payload.collections[collectionSlug]?.config
  if (!collectionConfig) {
    return
  }

  // TODO: Implement join resolution
}
```

**Step 2: Verify file compiles**

Run: `cd packages/db-clickhouse && pnpm run build`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add packages/db-clickhouse/src/utilities/resolveJoins.ts
git commit -m "feat(db-clickhouse): add resolveJoins utility skeleton"
```

---

### Task 2: Build join map from collection config

**Files:**

- Modify: `packages/db-clickhouse/src/utilities/resolveJoins.ts`

**Step 1: Add join map building logic**

After the early return checks, add:

```typescript
// Build map of join paths to their configurations
const joinMap: Record<string, { targetCollection: string } & SanitizedJoin> = {}

// Add regular joins (keyed by target collection)
for (const [targetCollection, joinList] of Object.entries(
  collectionConfig.joins || {},
)) {
  for (const join of joinList) {
    joinMap[join.joinPath] = { ...join, targetCollection }
  }
}

// Add polymorphic joins
for (const join of collectionConfig.polymorphicJoins || []) {
  const targetCollection = Array.isArray(join.field.collection)
    ? join.field.collection[0]
    : join.field.collection
  joinMap[join.joinPath] = { ...join, targetCollection }
}
```

**Step 2: Verify build**

Run: `cd packages/db-clickhouse && pnpm run build`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add packages/db-clickhouse/src/utilities/resolveJoins.ts
git commit -m "feat(db-clickhouse): build join map from collection config"
```

---

### Task 3: Query relationships table for referencing documents

**Files:**

- Modify: `packages/db-clickhouse/src/utilities/resolveJoins.ts`

**Step 1: Add helper to query relationships**

Add this helper function before the main `resolveJoins` function:

```typescript
interface RelationshipMatch {
  fromId: string
  fromType: string
  toId: string
}

async function queryRelationships(
  adapter: ClickHouseAdapter,
  options: {
    fromField: string
    fromTypes: string[]
    locale?: null | string
    toIds: string[]
    toType: string
  },
): Promise<RelationshipMatch[]> {
  const { fromField, fromTypes, locale, toIds, toType } = options

  if (toIds.length === 0 || fromTypes.length === 0) {
    return []
  }

  // Build IN clause for toIds
  const toIdParams: Record<string, string> = {}
  const toIdPlaceholders: string[] = []
  toIds.forEach((id, i) => {
    const paramName = `toId_${i}`
    toIdParams[paramName] = id
    toIdPlaceholders.push(`{${paramName}:String}`)
  })

  // Build IN clause for fromTypes
  const fromTypeParams: Record<string, string> = {}
  const fromTypePlaceholders: string[] = []
  fromTypes.forEach((type, i) => {
    const paramName = `fromType_${i}`
    fromTypeParams[paramName] = type
    fromTypePlaceholders.push(`{${paramName}:String}`)
  })

  const localeCondition = locale ? `AND locale = {locale:String}` : ''

  const query = `
    SELECT DISTINCT fromId, fromType, toId
    FROM (
      SELECT *,
        row_number() OVER (
          PARTITION BY ns, fromType, fromId, fromField, toType, toId, position
          ORDER BY v DESC
        ) as _rn
      FROM ${adapter.table}_relationships
      WHERE ns = {ns:String}
        AND toType = {toType:String}
        AND toId IN (${toIdPlaceholders.join(', ')})
        AND fromType IN (${fromTypePlaceholders.join(', ')})
        AND fromField = {fromField:String}
        ${localeCondition}
    )
    WHERE _rn = 1 AND deletedAt IS NULL
  `

  const params: Record<string, string> = {
    ns: adapter.namespace,
    toType,
    fromField,
    ...toIdParams,
    ...fromTypeParams,
  }

  if (locale) {
    params.locale = locale
  }

  const result = await adapter.clickhouse!.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  return result.json<RelationshipMatch>()
}
```

**Step 2: Verify build**

Run: `cd packages/db-clickhouse && pnpm run build`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add packages/db-clickhouse/src/utilities/resolveJoins.ts
git commit -m "feat(db-clickhouse): add queryRelationships helper"
```

---

### Task 4: Process each join and attach results

**Files:**

- Modify: `packages/db-clickhouse/src/utilities/resolveJoins.ts`

**Step 1: Add main join processing loop**

Add at the end of the `resolveJoins` function:

```typescript
// Extract parent document IDs
const parentIds = docs.map((doc) =>
  versions ? String(doc.parent ?? doc.id) : String(doc.id),
)

// Process each requested join
const joinPromises = Object.entries(joins).map(
  async ([joinPath, joinQuery]) => {
    if (!joinQuery || joinQuery === false) {
      return
    }

    const joinDef = joinMap[joinPath]
    if (!joinDef) {
      return
    }

    // Get target collections (array for polymorphic)
    const targetCollections = Array.isArray(joinDef.field.collection)
      ? joinDef.field.collection
      : [joinDef.field.collection]

    // Query relationships table
    const relationships = await queryRelationships(adapter, {
      fromField: joinDef.field.on,
      fromTypes: targetCollections,
      locale: locale ?? null,
      toIds: parentIds,
      toType: collectionSlug,
    })

    if (relationships.length === 0) {
      // Attach empty results to all docs
      for (const doc of docs) {
        doc[joinDef.field.name] = {
          docs: [],
          hasNextPage: false,
        }
      }
      return
    }

    // Group relationships by parent (toId)
    const relsByParent = new Map<string, RelationshipMatch[]>()
    for (const rel of relationships) {
      const existing = relsByParent.get(rel.toId) || []
      existing.push(rel)
      relsByParent.set(rel.toId, existing)
    }

    // Attach to documents (basic - without fetching full docs yet)
    const isPolymorphic = Array.isArray(joinDef.field.collection)
    for (const doc of docs) {
      const docId = versions ? String(doc.parent ?? doc.id) : String(doc.id)
      const rels = relsByParent.get(docId) || []

      // Apply pagination
      const limit = joinQuery.limit ?? joinDef.field.defaultLimit ?? 10
      const page = joinQuery.page ?? 1
      const skip = (page - 1) * limit
      const sliced = limit === 0 ? rels : rels.slice(skip, skip + limit)
      const hasNextPage = limit !== 0 && rels.length > skip + limit

      doc[joinDef.field.name] = {
        docs: sliced.map((rel) =>
          isPolymorphic
            ? { relationTo: rel.fromType, value: rel.fromId }
            : rel.fromId,
        ),
        hasNextPage,
        ...(joinQuery.count ? { totalDocs: rels.length } : {}),
      }
    }
  },
)

await Promise.all(joinPromises)
```

**Step 2: Verify build**

Run: `cd packages/db-clickhouse && pnpm run build`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add packages/db-clickhouse/src/utilities/resolveJoins.ts
git commit -m "feat(db-clickhouse): process joins and attach results"
```

---

### Task 5: Integrate with find.ts

**Files:**

- Modify: `packages/db-clickhouse/src/operations/find.ts`

**Step 1: Import resolveJoins**

Add at the top with other imports:

```typescript
import { resolveJoins } from '../utilities/resolveJoins.js'
```

**Step 2: Extract joins from args**

Update the destructuring at the start of the function to include joins:

```typescript
const {
  collection: collectionSlug,
  joins,
  limit = 10,
  locale,
  page = 1,
  pagination = true,
  sort,
  where,
} = args
```

**Step 3: Call resolveJoins after building docs**

After `const docs = rowsToDocuments<T & TypeWithID>(parsedRows, numericID) as T[]`, add:

```typescript
// Resolve join fields
await resolveJoins({
  adapter: this,
  collectionSlug,
  docs: docs as Record<string, unknown>[],
  joins,
  locale,
})
```

**Step 4: Verify build**

Run: `cd packages/db-clickhouse && pnpm run build`
Expected: Build succeeds

**Step 5: Commit**

```bash
git add packages/db-clickhouse/src/operations/find.ts
git commit -m "feat(db-clickhouse): integrate resolveJoins with find"
```

---

### Task 6: Integrate with findOne.ts

**Files:**

- Modify: `packages/db-clickhouse/src/operations/findOne.ts`

**Step 1: Import resolveJoins**

Add at the top:

```typescript
import { resolveJoins } from '../utilities/resolveJoins.js'
```

**Step 2: Extract joins and locale from args**

Update destructuring:

```typescript
const { collection: collectionSlug, joins, locale, where } = args
```

**Step 3: Call resolveJoins before returning**

Replace the return statement with:

```typescript
const parsedRow = parseDataRow(rows[0]!)
const doc = rowToDocument<T>(parsedRow, numericID)

// Resolve join fields
await resolveJoins({
  adapter: this,
  collectionSlug,
  docs: [doc as Record<string, unknown>],
  joins,
  locale,
})

return doc
```

**Step 4: Verify build**

Run: `cd packages/db-clickhouse && pnpm run build`
Expected: Build succeeds

**Step 5: Commit**

```bash
git add packages/db-clickhouse/src/operations/findOne.ts
git commit -m "feat(db-clickhouse): integrate resolveJoins with findOne"
```

---

### Task 7: Integrate with findGlobal.ts

**Files:**

- Modify: `packages/db-clickhouse/src/operations/findGlobal.ts`

**Step 1: Import resolveJoins**

Add at the top:

```typescript
import { resolveJoins } from '../utilities/resolveJoins.js'
```

**Step 2: Extract joins and locale from args**

Update destructuring:

```typescript
const { joins, locale, slug, where } = args
```

**Step 3: Call resolveJoins before returning**

Before the final return statement (around line 67), add:

```typescript
// Resolve join fields (globals use slug as collection identifier)
await resolveJoins({
  adapter: this,
  collectionSlug: slug,
  docs: [doc as unknown as Record<string, unknown>],
  joins,
  locale,
})
```

**Step 4: Verify build**

Run: `cd packages/db-clickhouse && pnpm run build`
Expected: Build succeeds

**Step 5: Commit**

```bash
git add packages/db-clickhouse/src/operations/findGlobal.ts
git commit -m "feat(db-clickhouse): integrate resolveJoins with findGlobal"
```

---

### Task 8: Integrate with findVersions.ts

**Files:**

- Modify: `packages/db-clickhouse/src/operations/findVersions.ts`

**Step 1: Import resolveJoins**

Add at the top:

```typescript
import { resolveJoins } from '../utilities/resolveJoins.js'
```

**Step 2: Extract joins and locale from args**

Update destructuring:

```typescript
const {
  collection: collectionSlug,
  joins,
  limit = 10,
  locale,
  page = 1,
  pagination = true,
  sort,
  where,
} = args
```

**Step 3: Call resolveJoins after building docs**

After the `docs.map()` that builds the version documents, add:

```typescript
// Resolve join fields for versions
await resolveJoins({
  adapter: this,
  collectionSlug,
  docs: docs as unknown as Record<string, unknown>[],
  joins,
  locale,
  versions: true,
})
```

**Step 4: Verify build**

Run: `cd packages/db-clickhouse && pnpm run build`
Expected: Build succeeds

**Step 5: Commit**

```bash
git add packages/db-clickhouse/src/operations/findVersions.ts
git commit -m "feat(db-clickhouse): integrate resolveJoins with findVersions"
```

---

### Task 9: Integrate with findGlobalVersions.ts

**Files:**

- Modify: `packages/db-clickhouse/src/operations/findGlobalVersions.ts`

**Step 1: Check current file structure**

Read the file to understand its structure. The pattern will be similar to findVersions.

**Step 2: Import resolveJoins**

Add at the top:

```typescript
import { resolveJoins } from '../utilities/resolveJoins.js'
```

**Step 3: Extract joins and locale, then call resolveJoins**

Similar pattern to findVersions - extract from args and call after building docs.

**Step 4: Verify build**

Run: `cd packages/db-clickhouse && pnpm run build`
Expected: Build succeeds

**Step 5: Commit**

```bash
git add packages/db-clickhouse/src/operations/findGlobalVersions.ts
git commit -m "feat(db-clickhouse): integrate resolveJoins with findGlobalVersions"
```

---

### Task 10: Run integration tests

**Files:**

- Test: `test/joins/int.spec.ts`

**Step 1: Run joins test suite**

Run: `pnpm run test:int joins`
Expected: Tests pass

**Step 2: If tests fail, debug and fix**

Check test output for specific failures. Common issues:

- Missing locale handling
- Incorrect ID extraction for versions
- Polymorphic join result format

**Step 3: Final commit if fixes needed**

```bash
git add -A
git commit -m "fix(db-clickhouse): address join resolution issues from tests"
```

---

### Task 11: Build and verify

**Step 1: Full build**

Run: `cd packages/db-clickhouse && pnpm run build`
Expected: Build succeeds with no errors

**Step 2: Run broader test suite**

Run: `pnpm run test:int database`
Expected: All tests pass

**Step 3: Final verification commit**

```bash
git add -A
git commit -m "feat(db-clickhouse): complete resolveJoins implementation"
```
