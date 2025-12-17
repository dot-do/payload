# ClickHouse Resolve Joins Design

## Overview

Implement `resolveJoins()` for the ClickHouse adapter to efficiently populate Join fields using the relationships table. This enables reverse lookups (e.g., "find all posts that reference this category") without full table scans.

## Background

Join fields are virtual fields that perform reverse lookups:

- A `posts` collection has a `category` relationship field pointing to `categories`
- A Join field on `categories` finds all posts that reference a given category

Without the relationships table, this requires scanning all documents and extracting JSON:

```sql
WHERE JSONExtract(data, 'category', 'String') = {categoryId}
```

The relationships table is indexed by `(ns, toType, toId, ...)` making reverse lookups efficient.

## Design

### Approach: Post-Query Resolution

Following MongoDB's pattern, `resolveJoins()` is called after the main `find()` returns documents:

```typescript
// In find.ts
const docs = rowsToDocuments(parsedRows, numericID)

await resolveJoins({
  adapter: this,
  collectionSlug,
  docs,
  joins,
  locale: args.locale,
})
```

This approach was chosen over query-time subqueries (Drizzle pattern) because:

1. Matches the adapter's simpler query pattern
2. ClickHouse's ReplacingMergeTree with window functions is complex in subqueries
3. Separate queries allow independent optimization
4. MongoDB uses this approach successfully

### Function Signature

```typescript
// utilities/resolveJoins.ts

export type ResolveJoinsArgs = {
  adapter: ClickHouseAdapter
  collectionSlug: string
  docs: Record<string, unknown>[]
  joins?: JoinQuery
  locale?: string
  versions?: boolean
}

export async function resolveJoins(args: ResolveJoinsArgs): Promise<void>
```

The function mutates `docs` in place to attach join results.

### Resolution Flow

For each Join field:

1. **Extract parent IDs** from the docs array

2. **Query relationships table** for documents pointing TO these IDs:

```sql
SELECT fromId
FROM (
  SELECT *,
    row_number() OVER (
      PARTITION BY ns, fromType, fromId, fromField, toType, toId, position
      ORDER BY v DESC
    ) as _rn
  FROM {table}_relationships
  WHERE ns = {ns}
    AND toType = {collectionSlug}
    AND toId IN ({parentIDs})
    AND fromType = {joinCollection}
    AND fromField = {onField}
)
WHERE _rn = 1 AND deletedAt IS NULL
```

3. **Fetch actual documents** from main table:

```sql
SELECT * EXCEPT(_rn)
FROM (
  SELECT *, row_number() OVER (PARTITION BY ns, type, id ORDER BY v DESC) as _rn
  FROM {table}
  WHERE ns = {ns}
    AND type = {joinCollection}
    AND id IN ({fromIds})
)
WHERE _rn = 1 AND deletedAt IS NULL
ORDER BY {sort}
LIMIT {limit + 1}
OFFSET {offset}
```

4. **Group by parent ID** and attach to docs with pagination metadata

### Pagination

- `limit` from joinQuery, defaults to field's `defaultLimit` or 10
- `page` calculates offset: `(page - 1) * limit`
- Fetch `limit + 1` to determine `hasNextPage`, then slice
- If `count: true`, run separate count query

### Result Structure

```typescript
doc[joinField.name] = {
  docs: results,           // IDs or {relationTo, value} for polymorphic
  hasNextPage: boolean,
  totalDocs?: number,      // if count requested
}
```

### Special Cases

**Localized relationships:**

- Filter by `locale` column when relationship field is localized
- Result path includes locale suffix: `joinField.en`

**Polymorphic collection joins** (`collection: ['posts', 'pages']`):

- Query relationships table with `fromType IN (collections)`
- Fetch docs from each collection separately
- Merge and sort results in memory
- Results as `{ relationTo: 'posts', value: id }`

**Polymorphic target relationships** (`relationTo: ['users', 'teams']`):

- Relationships table stores `toType` for each relationship
- Query filters by `toType = {collectionSlug}` automatically

**Versions:**

- When `versions: true`, query version tables
- Use `parent` field instead of `id` for document references

### Integration Points

Files to modify:

1. `utilities/resolveJoins.ts` - New file with core resolution logic
2. `operations/find.ts` - Call `resolveJoins()` after fetching docs
3. `operations/findOne.ts` - Call for single document
4. `operations/findGlobal.ts` - Call for globals with join fields
5. `operations/findVersions.ts` - Handle versions
6. `operations/findGlobalVersions.ts` - Handle global versions

## Implementation Tasks

1. Create `resolveJoins.ts` with core resolution logic
2. Add helper to query relationships table by toId
3. Add helper to fetch documents by IDs with sorting/pagination
4. Handle polymorphic collection joins
5. Handle localized join fields
6. Integrate with `find.ts`
7. Integrate with `findOne.ts`
8. Integrate with `findGlobal.ts`
9. Integrate with `findVersions.ts` and `findGlobalVersions.ts`
10. Test with existing joins test suite
