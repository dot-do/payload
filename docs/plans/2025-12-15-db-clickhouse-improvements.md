# db-clickhouse Improvements Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix findGlobal access control to use QueryBuilder, verify shared tests pass, and add ClickHouse-specific edge case tests.

**Architecture:** The findGlobal operation currently fetches the global document then applies access control filtering in-memory with an incomplete `matchesWhere` function. We'll move filtering into the SQL query using the existing QueryBuilder, which already supports all operators.

**Tech Stack:** TypeScript, ClickHouse, Jest

---

## Task 1: Fix findGlobal to Use QueryBuilder for Access Control

**Files:**

- Modify: `packages/db-clickhouse/src/operations/findGlobal.ts`

**Context:** The current `matchesWhere` function only supports `equals`, `not_equals`, and `exists` operators. Access control policies using `greater_than`, `in`, `contains`, etc. silently pass. The QueryBuilder already handles all operators - we should use it in the SQL query instead of filtering in memory.

**Step 1: Read the current findGlobal implementation**

Understand the current flow before modifying.

**Step 2: Remove the matchesWhere function and update findGlobal**

Replace the entire file with:

```typescript
import type { FindGlobal, FindGlobalArgs } from 'payload'

import type { QueryParams } from '../queries/QueryBuilder.js'
import type { ClickHouseAdapter, DataRow } from '../types.js'

import { combineWhere, QueryBuilder } from '../queries/QueryBuilder.js'
import { assertValidSlug } from '../utilities/sanitize.js'
import { parseDataRow, rowToDocument } from '../utilities/transform.js'

const GLOBALS_TYPE = '_globals'

export const findGlobal: FindGlobal = async function findGlobal<
  T extends Record<string, unknown> = Record<string, unknown>,
>(this: ClickHouseAdapter, args: FindGlobalArgs): Promise<T> {
  const { slug, where } = args

  assertValidSlug(slug, 'global')

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const qb = new QueryBuilder()

  // Build base WHERE for this specific global
  const nsParam = qb.addNamedParam('ns', this.namespace)
  const typeParam = qb.addNamedParam('type', GLOBALS_TYPE)
  const idParam = qb.addNamedParam('id', slug)
  let baseWhere = `ns = ${nsParam} AND type = ${typeParam} AND id = ${idParam}`

  // Add access control conditions if provided
  const accessControlWhere = qb.buildWhereClause(where)
  if (accessControlWhere) {
    baseWhere = combineWhere(baseWhere, accessControlWhere)
  }

  const params = qb.getParams()

  // Use window function to get latest version, then filter by access control and deletedAt
  const query = `
    SELECT * EXCEPT(_rn)
    FROM (
      SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
      FROM ${this.table}
      WHERE ns = {ns:String}
        AND type = {type:String}
        AND id = {id:String}
    )
    WHERE _rn = 1 AND deletedAt IS NULL${accessControlWhere ? ` AND (${accessControlWhere})` : ''}
    LIMIT 1
  `

  const result = await this.clickhouse.query({
    format: 'JSONEachRow',
    query,
    query_params: params,
  })

  const rows = await result.json<DataRow>()

  if (rows.length === 0) {
    // No document found - either doesn't exist or access denied
    // Return empty object if there's a where clause (access control denied)
    // Otherwise return base object (global doesn't exist yet)
    if (where && Object.keys(where).length > 0) {
      return {} as T
    }
    return { id: slug, globalType: slug } as unknown as T
  }

  const parsedRow = parseDataRow(rows[0]!)
  const doc = rowToDocument<{ id: string } & T>(parsedRow)

  return { ...doc, globalType: slug } as unknown as T
}
```

**Step 3: Run linting to verify syntax**

Run: `pnpm run lint packages/db-clickhouse/src/operations/findGlobal.ts`
Expected: No errors

**Step 4: Commit**

```bash
git add packages/db-clickhouse/src/operations/findGlobal.ts
git commit -m "fix(db-clickhouse): use QueryBuilder for findGlobal access control

Previously matchesWhere only supported equals, not_equals, exists.
Now uses QueryBuilder which supports all operators including
greater_than, in, contains, like, near, within, etc."
```

---

## Task 2: Verify Shared Tests Pass with ClickHouse

**Files:**

- None (test run only)

**Context:** The shared test suite in `test/` exercises all database adapters. ClickHouse is configured in `test/generateDatabaseAdapter.ts`. We need to verify tests pass.

**Step 1: Start ClickHouse (if not running)**

Check docker-compose.yml for ClickHouse service:

Run: `docker compose -f test/docker-compose.yml up -d clickhouse`
Expected: ClickHouse container starts

**Step 2: Run a subset of integration tests with ClickHouse**

Start with the database-specific tests:

Run: `PAYLOAD_DATABASE=clickhouse pnpm test:int database`
Expected: Tests pass (some may be skipped if they're DB-specific)

**Step 3: Run field tests**

Run: `PAYLOAD_DATABASE=clickhouse pnpm test:int fields`
Expected: Tests pass

**Step 4: Run globals tests**

This will exercise our findGlobal fix:

Run: `PAYLOAD_DATABASE=clickhouse pnpm test:int globals`
Expected: Tests pass

**Step 5: Document any failures**

If tests fail, note them for Task 3 to address as ClickHouse-specific edge cases.

---

## Task 3: Add ClickHouse-Specific Test Helpers

**Files:**

- Create: `test/helpers/isClickHouse.ts`

**Context:** Some tests may need to be skipped or adjusted for ClickHouse due to eventual consistency. Create a helper similar to `isMongoose.ts`.

**Step 1: Create the isClickHouse helper**

```typescript
import type { Payload } from 'payload'

export function isClickHouse(payload: Payload): boolean {
  return payload.db.name === 'clickhouse'
}
```

**Step 2: Commit**

```bash
git add test/helpers/isClickHouse.ts
git commit -m "test: add isClickHouse helper for conditional test logic"
```

---

## Task 4: Add ClickHouse Eventual Consistency Test

**Files:**

- Create: `packages/db-clickhouse/src/__tests__/eventual-consistency.spec.ts`

**Context:** ClickHouse uses ReplacingMergeTree which provides eventual consistency. Tests should verify the adapter handles this correctly via window functions.

**Step 1: Create test directory**

Run: `mkdir -p packages/db-clickhouse/src/__tests__`

**Step 2: Create the eventual consistency test**

```typescript
import { describe, expect, it } from 'vitest'

import { generateVersion } from '../utilities/generateId.js'

describe('eventual consistency', () => {
  describe('generateVersion', () => {
    it('should generate monotonically increasing versions', () => {
      const versions: number[] = []
      for (let i = 0; i < 100; i++) {
        versions.push(generateVersion())
      }

      // Each version should be greater than the previous
      for (let i = 1; i < versions.length; i++) {
        expect(versions[i]).toBeGreaterThan(versions[i - 1]!)
      }
    })

    it('should generate versions based on current time', () => {
      const before = Date.now()
      const version = generateVersion()
      const after = Date.now()

      expect(version).toBeGreaterThanOrEqual(before)
      expect(version).toBeLessThanOrEqual(after + 100) // Allow small drift
    })

    it('should handle rapid calls within same millisecond', () => {
      // Generate many versions rapidly
      const versions = new Set<number>()
      for (let i = 0; i < 1000; i++) {
        versions.add(generateVersion())
      }

      // All versions should be unique
      expect(versions.size).toBe(1000)
    })
  })
})
```

**Step 3: Add vitest config for db-clickhouse if needed**

Check if `packages/db-clickhouse/vitest.config.ts` exists. If not, create:

```typescript
import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
  },
})
```

**Step 4: Run the test**

Run: `cd packages/db-clickhouse && pnpm vitest run`
Expected: Tests pass

**Step 5: Commit**

```bash
git add packages/db-clickhouse/src/__tests__/
git add packages/db-clickhouse/vitest.config.ts
git commit -m "test(db-clickhouse): add eventual consistency unit tests

Tests verify generateVersion produces monotonically increasing
unique values even under rapid concurrent calls."
```

---

## Task 5: Add QueryBuilder Unit Tests

**Files:**

- Create: `packages/db-clickhouse/src/__tests__/QueryBuilder.spec.ts`

**Context:** The QueryBuilder is critical for SQL generation. Add unit tests for complex operators.

**Step 1: Create QueryBuilder tests**

```typescript
import { describe, expect, it } from 'vitest'

import { combineWhere, QueryBuilder } from '../queries/QueryBuilder.js'

describe('QueryBuilder', () => {
  describe('buildWhereClause', () => {
    it('should handle equals operator', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({ title: { equals: 'test' } })
      expect(where).toContain('data.title')
      expect(where).toContain('=')
    })

    it('should handle in operator with array', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({
        status: { in: ['draft', 'published'] },
      })
      expect(where).toContain('IN')
    })

    it('should handle empty in array', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({ status: { in: [] } })
      expect(where).toBe('1=0') // Should never match
    })

    it('should handle greater_than operator', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({ count: { greater_than: 10 } })
      expect(where).toContain('>')
    })

    it('should handle nested AND conditions', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({
        and: [
          { title: { equals: 'test' } },
          { status: { equals: 'published' } },
        ],
      })
      expect(where).toContain('AND')
    })

    it('should handle nested OR conditions', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({
        or: [{ title: { equals: 'a' } }, { title: { equals: 'b' } }],
      })
      expect(where).toContain('OR')
    })

    it('should handle null equals', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({ deletedAt: { equals: null } })
      expect(where).toContain('IS NULL')
    })

    it('should handle exists operator', () => {
      const qb = new QueryBuilder()
      const whereExists = qb.buildWhereClause({ field: { exists: true } })
      expect(whereExists).toContain('IS NOT NULL')

      const qb2 = new QueryBuilder()
      const whereNotExists = qb2.buildWhereClause({ field: { exists: false } })
      expect(whereNotExists).toContain('IS NULL')
    })

    it('should handle contains operator', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({ title: { contains: 'test' } })
      expect(where).toContain('position')
      expect(where).toContain('lower')
    })

    it('should handle like operator', () => {
      const qb = new QueryBuilder()
      const where = qb.buildWhereClause({ title: { like: 'test' } })
      expect(where).toContain('ILIKE')
    })
  })

  describe('combineWhere', () => {
    it('should combine base and additional where', () => {
      const result = combineWhere('ns = {ns:String}', 'status = {p0:String}')
      expect(result).toBe('ns = {ns:String} AND (status = {p0:String})')
    })

    it('should return base when additional is empty', () => {
      const result = combineWhere('ns = {ns:String}', '')
      expect(result).toBe('ns = {ns:String}')
    })
  })

  describe('getParams', () => {
    it('should collect all parameters', () => {
      const qb = new QueryBuilder()
      qb.buildWhereClause({
        title: { equals: 'test' },
        count: { greater_than: 5 },
      })
      const params = qb.getParams()
      expect(Object.keys(params).length).toBeGreaterThan(0)
    })
  })
})
```

**Step 2: Run the tests**

Run: `cd packages/db-clickhouse && pnpm vitest run`
Expected: Tests pass

**Step 3: Commit**

```bash
git add packages/db-clickhouse/src/__tests__/QueryBuilder.spec.ts
git commit -m "test(db-clickhouse): add QueryBuilder unit tests

Covers equals, in, greater_than, AND/OR conditions, null handling,
exists, contains, like operators and parameter collection."
```

---

## Task 6: Add Soft Delete Edge Case Test

**Files:**

- Modify: `packages/db-clickhouse/src/__tests__/eventual-consistency.spec.ts`

**Context:** Soft deletes are critical for ClickHouse's ReplacingMergeTree. Verify the pattern works correctly.

**Step 1: Add soft delete test cases**

Append to `packages/db-clickhouse/src/__tests__/eventual-consistency.spec.ts`:

```typescript
describe('soft delete pattern', () => {
  it('should use window function to get latest version', () => {
    // This tests the SQL pattern used throughout the adapter
    // The pattern: row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
    // WHERE _rn = 1 AND deletedAt IS NULL

    // Simulate the data model
    const rows = [
      { id: '1', v: 100, deletedAt: null, data: 'v1' },
      { id: '1', v: 200, deletedAt: null, data: 'v2' },
      { id: '1', v: 300, deletedAt: '2024-01-01', data: 'deleted' }, // soft deleted
    ]

    // Apply window function logic
    const grouped = new Map<string, typeof rows>()
    for (const row of rows) {
      const existing = grouped.get(row.id) || []
      existing.push(row)
      grouped.set(row.id, existing)
    }

    // Get latest non-deleted for each id
    const results: typeof rows = []
    for (const [, group] of grouped) {
      const sorted = group.sort((a, b) => b.v - a.v)
      const latest = sorted.find((r) => r.deletedAt === null)
      if (latest) {
        results.push(latest)
      }
    }

    // Should get v2 (latest non-deleted)
    expect(results.length).toBe(1)
    expect(results[0]!.data).toBe('v2')
    expect(results[0]!.v).toBe(200)
  })
})
```

**Step 2: Run tests**

Run: `cd packages/db-clickhouse && pnpm vitest run`
Expected: Tests pass

**Step 3: Commit**

```bash
git add packages/db-clickhouse/src/__tests__/eventual-consistency.spec.ts
git commit -m "test(db-clickhouse): add soft delete pattern test

Verifies window function logic correctly returns latest
non-deleted version of a document."
```

---

## Task 7: Run Full Integration Test Suite

**Files:**

- None (test run only)

**Context:** Final verification that all changes work together.

**Step 1: Run core integration tests**

Run: `PAYLOAD_DATABASE=clickhouse pnpm test:int auth`
Expected: Tests pass

**Step 2: Run access control tests**

This exercises the findGlobal fix with access control:

Run: `PAYLOAD_DATABASE=clickhouse pnpm test:int access-control`
Expected: Tests pass

**Step 3: Document results**

If any tests fail, they may need ClickHouse-specific handling. Note them for follow-up.

---

## Summary

| Task | Description                        | Status  |
| ---- | ---------------------------------- | ------- |
| 1    | Fix findGlobal to use QueryBuilder | Pending |
| 2    | Verify shared tests pass           | Pending |
| 3    | Add isClickHouse helper            | Pending |
| 4    | Add eventual consistency tests     | Pending |
| 5    | Add QueryBuilder unit tests        | Pending |
| 6    | Add soft delete pattern test       | Pending |
| 7    | Run full integration test suite    | Pending |
