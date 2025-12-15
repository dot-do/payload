# ClickHouse Plugin Design

**Date:** 2025-12-15
**Status:** Draft

## Overview

A Payload plugin that exposes ClickHouse tables managed by `db-clickhouse` as browsable collections in the admin UI, plus automatic hooks for seamless integration.

The plugin adds **5 collections** that provide Payload interfaces over existing ClickHouse tables:

| Collection      | Purpose                           | Admin UI                                      |
| --------------- | --------------------------------- | --------------------------------------------- |
| `search`        | Full-text + vector search index   | Browse indexed content, view embedding status |
| `events`        | Audit log of all tracked activity | Filter/search event history                   |
| `relationships` | Document link graph               | Explore connections, find orphans             |
| `actions`       | Task/job orchestration queue      | Monitor jobs, retry failed tasks              |
| `data`          | Raw document browser (optional)   | Debug view of underlying ClickHouse data      |

**Key design decision:** These collections are **virtual** - they don't create new ClickHouse tables. Instead, they provide Payload collection interfaces over the existing tables that `db-clickhouse` already creates.

## Configuration

### Top-Level Options

```typescript
clickhousePlugin({
  // Top-level admin group (default for all collections)
  adminGroup: 'Analytics',

  search: {
    /* ... */
  },
  events: {
    /* ... */
  },
  relationships: {
    /* ... */
  },
  actions: {
    /* ... */
  },
  data: false, // disabled
})
```

### Resolution Order

**For `adminGroup`:**

1. Collection-specific `adminGroup` if set
2. Top-level `adminGroup` if set
3. Default: `'ClickHouse'`

**For `slug`:**

1. Collection-specific `slug` if set
2. Default: `'search'`, `'events'`, `'relationships'`, `'actions'`, `'data'`

---

## Search Collection

### Schema

```typescript
{
  slug: 'search',
  labels: { singular: 'Search Index', plural: 'Search Index' },
  admin: { group: 'ClickHouse' },
  fields: [
    { name: 'collection', type: 'text', required: true },
    { name: 'docId', type: 'text', required: true },
    { name: 'text', type: 'textarea' },
    { name: 'chunkIndex', type: 'number', defaultValue: 0 },
    { name: 'status', type: 'select', options: ['pending', 'ready', 'failed'] },
    { name: 'error', type: 'text' },
  ]
}
```

### Configuration

```typescript
search: {
  enabled: true,
  slug: 'search-index',        // override collection slug
  adminGroup: 'Search',        // override group for this collection only

  // Opt-in collections with field configuration
  collections: {
    posts: {
      fields: ['title', 'content', 'excerpt'],
      titleField: 'title',
    },
    pages: {
      fields: ['title', 'body.root'],  // supports nested paths
    }
  },

  // OR: index all collections with default text extraction
  indexAll: {
    enabled: false,
    defaultFields: ['title', 'name', 'content', 'description'],
  },

  // Chunking for long documents
  chunkSize: 1000,
  chunkOverlap: 100,
}
```

### Hooks

Hooks added to tracked collections:

- `afterChange` → calls `syncToSearch()` to index/re-index
- `afterDelete` → removes from search index

---

## Events Collection

### Schema

```typescript
{
  slug: 'events',
  labels: { singular: 'Event', plural: 'Events' },
  admin: { group: 'ClickHouse' },
  access: {
    create: () => false,  // immutable
    update: () => false,  // immutable
    delete: () => false,  // immutable
    read: ({ req }) => req.user?.role === 'admin',
  },
  fields: [
    { name: 'type', type: 'text', required: true },
    { name: 'collection', type: 'text' },
    { name: 'docId', type: 'text' },
    { name: 'userId', type: 'text' },
    { name: 'sessionId', type: 'text' },
    { name: 'ip', type: 'text' },
    { name: 'input', type: 'json' },
    { name: 'result', type: 'json' },
    { name: 'duration', type: 'number' },
    { name: 'timestamp', type: 'date' },
  ]
}
```

### Configuration

```typescript
events: {
  enabled: true,
  slug: 'audit-log',

  // What to track automatically
  trackCRUD: true,           // doc.create, doc.update, doc.delete
  trackAuth: true,           // auth.login, auth.logout, auth.failed

  // Per-collection overrides
  collections: {
    users: { track: true, includeInput: false },
    posts: { track: true },
    sessions: { track: false },
  },

  // Access control
  access: {
    read: ({ req }) => req.user?.roles?.includes('auditor'),
  },
}
```

### Custom Tracking API

```typescript
// Simple tracking (not tied to a collection)
await payload.db.track('checkout.completed', {
  orderId: '123',
  total: 99.99,
})

// With full options
await payload.db.track('ai.embedding.generated', {
  collection: 'posts',
  docId: 'post-123',
  input: { model: 'text-embedding-3-small' },
  result: { dimensions: 1536, tokens: 245 },
  duration: 340,
})
```

---

## Relationships Collection

### Schema

```typescript
{
  slug: 'relationships',
  labels: { singular: 'Relationship', plural: 'Relationships' },
  admin: { group: 'ClickHouse' },
  access: {
    create: () => false,
    update: () => false,
    delete: () => false,
    read: ({ req }) => !!req.user,
  },
  fields: [
    { name: 'fromType', type: 'text', required: true },
    { name: 'fromId', type: 'text', required: true },
    { name: 'fromField', type: 'text', required: true },
    { name: 'toType', type: 'text', required: true },
    { name: 'toId', type: 'text', required: true },
    { name: 'position', type: 'number' },
    { name: 'locale', type: 'text' },
  ]
}
```

### Configuration

```typescript
relationships: {
  enabled: true,
  slug: 'document-links',
  adminGroup: 'Debug',
  access: {
    read: ({ req }) => req.user?.role === 'admin',
  },
}
```

### Query Helpers

```typescript
// Find all documents linking TO a specific document
await payload.db.getIncomingLinks({
  collection: 'authors',
  id: 'author-123',
})

// Find all documents a specific document links TO
await payload.db.getOutgoingLinks({
  collection: 'posts',
  id: 'post-123',
})

// Find orphaned references
await payload.db.findOrphanedLinks({
  collection: 'posts',
})

// Graph traversal
await payload.db.traverseGraph({
  collection: 'posts',
  id: 'post-123',
  depth: 2,
  direction: 'both',
})
```

---

## Actions Collection

### Schema

```typescript
{
  slug: 'actions',
  labels: { singular: 'Action', plural: 'Actions' },
  admin: { group: 'ClickHouse' },
  fields: [
    // Identity
    { name: 'type', type: 'text', required: true },
    { name: 'name', type: 'text', required: true },

    // Status
    { name: 'status', type: 'select', required: true,
      options: ['pending', 'running', 'waiting', 'completed', 'failed', 'cancelled'] },
    { name: 'priority', type: 'number', defaultValue: 0 },

    // Context
    { name: 'collection', type: 'text' },
    { name: 'docId', type: 'text' },

    // Payload
    { name: 'input', type: 'json' },
    { name: 'output', type: 'json' },
    { name: 'error', type: 'json' },

    // Workflow state
    { name: 'step', type: 'number', defaultValue: 0 },
    { name: 'steps', type: 'json' },
    { name: 'context', type: 'json' },

    // Human-in-the-loop
    { name: 'assignedTo', type: 'text' },
    { name: 'waitingFor', type: 'select',
      options: ['input', 'approval', 'review', 'external'] },

    // Timing
    { name: 'scheduledAt', type: 'date' },
    { name: 'startedAt', type: 'date' },
    { name: 'completedAt', type: 'date' },
    { name: 'timeoutAt', type: 'date' },

    // Retry
    { name: 'attempts', type: 'number', defaultValue: 0 },
    { name: 'maxAttempts', type: 'number', defaultValue: 3 },
    { name: 'retryAfter', type: 'date' },

    // Lineage
    { name: 'parentId', type: 'text' },
    { name: 'rootId', type: 'text' },
  ]
}
```

### Configuration

```typescript
actions: {
  enabled: true,
  slug: 'tasks',

  retention: {
    completed: '30d',
    failed: '90d',
  },

  access: {
    read: ({ req }) => !!req.user,
    update: ({ req }) => req.user?.role === 'admin',
    delete: ({ req }) => req.user?.role === 'admin',
  },
}
```

### Task API

**Creating jobs:**

```typescript
// Simple background job
await payload.db.enqueue({
  name: 'generate-embedding',
  input: { collection: 'posts', docId: 'post-123', text: '...' },
  priority: 10,
})

// Delayed job
await payload.db.enqueue({
  name: 'send-reminder-email',
  input: { userId: 'user-123', template: 'weekly-digest' },
  scheduledAt: new Date(Date.now() + 86400000),
})

// Batch jobs
await payload.db.enqueueBatch([
  { name: 'generate-embedding', input: { docId: 'post-1' } },
  { name: 'generate-embedding', input: { docId: 'post-2' } },
  { name: 'generate-embedding', input: { docId: 'post-3' } },
])

// Human-in-the-loop task
await payload.db.enqueue({
  name: 'approve-post',
  type: 'task',
  collection: 'posts',
  docId: 'post-123',
  assignedTo: 'editor-user-id',
  waitingFor: 'approval',
  input: { message: 'Please review before publishing' },
})

// Multi-step workflow
await payload.db.enqueue({
  name: 'enrich-document',
  type: 'workflow',
  collection: 'posts',
  docId: 'post-123',
  steps: [
    { name: 'extract-text', handler: 'extractText' },
    { name: 'generate-summary', handler: 'llmSummarize' },
    { name: 'generate-embedding', handler: 'generateEmbedding' },
    { name: 'human-review', handler: 'waitForApproval', waitingFor: 'review' },
    { name: 'publish', handler: 'setStatus', input: { status: 'published' } },
  ],
})
```

**Worker/Consumer API:**

```typescript
// Claim jobs
const jobs = await payload.db.claimActions({
  name: 'generate-embedding',
  limit: 10,
  lockFor: 60000,
})

// Complete
await payload.db.completeAction({
  id: 'action-123',
  output: { embedding: [...], tokens: 245 },
})

// Fail (will retry if attempts < maxAttempts)
await payload.db.failAction({
  id: 'action-123',
  error: { message: 'API rate limited', code: 'RATE_LIMIT' },
  retryAfter: new Date(Date.now() + 60000),
})

// Cancel
await payload.db.cancelAction({ id: 'action-123' })

// Resume (after human input)
await payload.db.resumeAction({
  id: 'action-123',
  input: { approved: true, notes: 'Looks good!' },
})
```

**Query helpers:**

```typescript
// Get pending actions for a document
await payload.db.getDocumentActions({
  collection: 'posts',
  docId: 'post-123',
  status: ['pending', 'running', 'waiting'],
})

// Get tasks assigned to a user
await payload.db.getAssignedTasks({
  userId: 'user-123',
  waitingFor: 'approval',
})
```

---

## Plugin Entry Point

```typescript
// packages/plugin-clickhouse/src/index.ts
import type { Config, Plugin } from 'payload'

export interface PluginClickHouseConfig {
  adminGroup?: string
  search?: false | SearchConfig
  events?: false | EventsConfig
  relationships?: false | RelationshipsConfig
  actions?: false | ActionsConfig
  data?: false | DataConfig
}

export const clickhousePlugin =
  (config: PluginClickHouseConfig): Plugin =>
  (incomingConfig: Config): Config => {
    const collections = [...(incomingConfig.collections || [])]
    const adminGroup = config.adminGroup ?? 'ClickHouse'

    if (config.search !== false) {
      collections.push(generateSearchCollection(config.search, adminGroup))
    }

    if (config.events !== false) {
      collections.push(generateEventsCollection(config.events, adminGroup))
    }

    if (config.relationships !== false) {
      collections.push(
        generateRelationshipsCollection(config.relationships, adminGroup),
      )
    }

    if (config.actions !== false) {
      collections.push(generateActionsCollection(config.actions, adminGroup))
    }

    if (config.data) {
      collections.push(generateDataCollection(config.data, adminGroup))
    }

    return {
      ...incomingConfig,
      collections,
    }
  }
```

---

## Full Usage Example

```typescript
// payload.config.ts
import { buildConfig } from 'payload'
import { clickhouseAdapter } from '@payloadcms/db-clickhouse'
import { clickhousePlugin } from '@payloadcms/plugin-clickhouse'

export default buildConfig({
  db: clickhouseAdapter({
    url: process.env.CLICKHOUSE_URL,
    database: 'myapp',
    namespace: 'production',
  }),

  plugins: [
    clickhousePlugin({
      adminGroup: 'Analytics',

      search: {
        slug: 'search-index',
        collections: {
          posts: { fields: ['title', 'content'] },
          pages: { fields: ['title', 'body'] },
        },
        chunkSize: 1000,
      },

      events: {
        slug: 'audit-log',
        trackCRUD: true,
        trackAuth: true,
        collections: {
          users: { includeInput: false },
          sessions: { track: false },
        },
      },

      relationships: {
        adminGroup: 'Debug',
      },

      actions: {
        slug: 'tasks',
        retention: { completed: '30d', failed: '90d' },
      },

      data: false,
    }),
  ],

  collections: [
    // your collections...
  ],
})
```

---

## Summary

| Feature           | Collection                  | Hooks                        | API Methods                                                                                               |
| ----------------- | --------------------------- | ---------------------------- | --------------------------------------------------------------------------------------------------------- |
| **Search**        | `search`                    | `afterChange`, `afterDelete` | `syncToSearch`, `search`, `getSearchQueue`, `updateSearchStatus`                                          |
| **Events**        | `events` (read-only)        | CRUD + Auth hooks            | `track()`, `queryEvents`                                                                                  |
| **Relationships** | `relationships` (read-only) | -                            | `getIncomingLinks`, `getOutgoingLinks`, `findOrphanedLinks`, `traverseGraph`                              |
| **Actions**       | `actions`                   | -                            | `enqueue`, `enqueueBatch`, `claimActions`, `completeAction`, `failAction`, `cancelAction`, `resumeAction` |
