# ClickHouse Relationships Table Design

## Overview

Add a dedicated relationships table to the ClickHouse adapter to optimize reverse lookups (Join fields). Currently, Join field queries require full table scans with JSON extraction. A relationships index enables efficient JOINs.

## Problem

**Current state**: Relationships are stored embedded in the JSON `data` column. Reverse lookups require:

```sql
SELECT * FROM data
WHERE type = 'posts'
  AND JSONExtract(data, 'author', 'String') = 'user-123'
```

This is slow because:

1. No index on relationship values inside JSON
2. Full scan of all documents of that type
3. JSON parsing for every row

**Comparison with other adapters**:
| Adapter | Storage | Reverse Lookup |
|---------|---------|----------------|
| MongoDB | Embedded fields | `$lookup` on indexed field |
| Drizzle | `_rels` junction tables | SQL JOINs on indexed FK |
| ClickHouse (current) | Embedded in JSON | Full scan + JSON extraction |

## Solution

### Schema

```sql
CREATE TABLE IF NOT EXISTS {table}_relationships (
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
```

**Column descriptions**:

- `ns` - Namespace (multi-tenant isolation)
- `fromType` - Source collection slug (e.g., 'posts')
- `fromId` - Source document ID
- `fromField` - Field name on source containing the relationship (e.g., 'category')
- `toType` - Target collection slug (e.g., 'categories')
- `toId` - Target document ID
- `position` - Order for hasMany arrays (0 for single relationships)
- `locale` - For localized relationships
- `v` - Version timestamp (for ReplacingMergeTree deduplication)
- `deletedAt` - Soft delete timestamp

**ORDER BY optimization**: Starts with `(toType, toId)` to optimize reverse lookups - "find all documents referencing this ID" becomes a range scan.

### Write Path

On create/update, extract relationships inline and insert to the relationships table:

```typescript
function extractRelationships(
  doc: Document,
  fields: Field[],
  options: {
    ns: string
    fromType: string
    fromId: string
    v: number
    locale?: string
  },
): RelationshipRow[] {
  const relationships: RelationshipRow[] = []

  for (const field of fields) {
    if (field.type === 'relationship' || field.type === 'upload') {
      const value = doc[field.name]
      if (!value) continue

      const relationTos = Array.isArray(field.relationTo)
        ? field.relationTo
        : [field.relationTo]

      if (field.hasMany && Array.isArray(value)) {
        value.forEach((item, position) => {
          const isPolymorphic = typeof item === 'object' && 'relationTo' in item
          relationships.push({
            ...options,
            fromField: field.name,
            toType: isPolymorphic ? item.relationTo : field.relationTo,
            toId: String(isPolymorphic ? item.value : item),
            position,
            deletedAt: null,
          })
        })
      } else {
        const isPolymorphic = typeof value === 'object' && 'relationTo' in value
        relationships.push({
          ...options,
          fromField: field.name,
          toType: isPolymorphic ? value.relationTo : field.relationTo,
          toId: String(isPolymorphic ? value.value : value),
          position: 0,
          deletedAt: null,
        })
      }
    }
  }
  return relationships
}
```

**Sync strategy**: Overwrite pattern (matches main data table)

- Insert new relationship rows with current `v` timestamp
- On read, use window function to get latest per (fromId, fromField, position)
- No need to query-then-delete; ReplacingMergeTree handles deduplication

### Read Path - Join Field Queries

For Join fields (reverse lookups), query the relationships table and JOIN to data:

```sql
-- Find all posts where category = {categoryId}
SELECT d.*
FROM data d FINAL
INNER JOIN (
    SELECT fromType, fromId
    FROM (
        SELECT *,
            row_number() OVER (
                PARTITION BY ns, fromType, fromId, fromField, toType, toId
                ORDER BY v DESC
            ) as _rn
        FROM {table}_relationships
        WHERE ns = {ns}
          AND toType = 'categories'
          AND toId = {categoryId}
          AND fromField = 'category'
    )
    WHERE _rn = 1 AND deletedAt IS NULL
) r ON d.ns = {ns} AND d.type = r.fromType AND d.id = r.fromId
WHERE d.ns = {ns} AND d.type = 'posts'
ORDER BY d.createdAt DESC
LIMIT 10
```

**Polymorphic relationships**: The `toType` column handles multiple target collections:

```sql
WHERE toType IN ('posts', 'pages') AND toId = {userId}
```

### Soft Delete Handling

When a document is soft-deleted:

1. Main data table gets a row with `deletedAt` set
2. Relationship rows should also be soft-deleted (same `v` timestamp)

This ensures deleted documents don't appear in Join field results.

## Implementation Tasks

1. **Schema**: Add table creation to `connect.ts`
2. **Types**: Add `RelationshipRow` type to `types.ts`
3. **Extract utility**: Create `extractRelationships()` in `utilities/relationships.ts`
4. **Write operations**: Update `create.ts`, `updateOne.ts`, `updateMany.ts` to sync relationships
5. **Delete operations**: Update `deleteOne.ts`, `deleteMany.ts` to soft-delete relationships
6. **Join query**: Add `findJoin()` operation or integrate with existing find
7. **Tests**: Add relationship extraction and Join field query tests

## Future Considerations

- **Nested relationships**: Currently storing just field name. May need full path for deeply nested fields (e.g., `blocks.0.author`)
- **Batch optimization**: Could batch relationship inserts for updateMany
- **Materialized view**: Could explore ClickHouse materialized views for auto-sync

## References

- MongoDB adapter: Uses `$lookup` aggregation with auto-indexed relationship fields
- Drizzle adapter: Uses `_rels` junction tables with composite indexes
- Payload Join field config: `{ type: 'join', collection: 'posts', on: 'category' }`
