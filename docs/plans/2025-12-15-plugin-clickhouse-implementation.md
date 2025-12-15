# ClickHouse Plugin Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create a Payload plugin that exposes ClickHouse tables as admin-browsable collections with automatic hooks integration.

**Architecture:** Plugin pattern matching `@payloadcms/plugin-search`. Virtual collections map to existing db-clickhouse tables. Hooks inject into user collections for automatic search/event tracking. API helpers extend `payload.db` for relationships and actions.

**Tech Stack:** TypeScript, Payload Plugin API, db-clickhouse adapter integration

---

## Parallel Task Groups

Tasks are organized into groups that can be executed in parallel:

| Group | Tasks                                 | Dependencies            |
| ----- | ------------------------------------- | ----------------------- |
| **A** | 1 (Package Setup)                     | None                    |
| **B** | 2 (Types)                             | A                       |
| **C** | 3, 4, 5, 6, 7 (Collection Generators) | B - can run in parallel |
| **D** | 8 (Plugin Entry Point)                | C                       |
| **E** | 9, 10 (Hooks)                         | D - can run in parallel |
| **F** | 11, 12, 13 (API Helpers)              | D - can run in parallel |
| **G** | 14 (Integration Test)                 | E, F                    |

---

## Task 1: Package Setup

**Files:**

- Create: `packages/plugin-clickhouse/package.json`
- Create: `packages/plugin-clickhouse/tsconfig.json`
- Create: `packages/plugin-clickhouse/.swcrc`
- Create: `packages/plugin-clickhouse/src/index.ts` (placeholder)

**Step 1: Create package.json**

```json
{
  "name": "@dotdo/plugin-clickhouse",
  "version": "0.0.1",
  "description": "ClickHouse plugin for Payload - exposes search, events, relationships, and actions collections",
  "keywords": [
    "payload",
    "cms",
    "plugin",
    "typescript",
    "clickhouse",
    "search",
    "events",
    "analytics"
  ],
  "repository": {
    "type": "git",
    "url": "https://github.com/payloadcms/payload.git",
    "directory": "packages/plugin-clickhouse"
  },
  "license": "MIT",
  "author": "Payload <dev@payloadcms.com> (https://payloadcms.com)",
  "sideEffects": false,
  "type": "module",
  "exports": {
    ".": {
      "types": "./src/index.ts",
      "default": "./src/index.ts"
    },
    "./types": {
      "import": "./src/types.ts",
      "types": "./src/types.ts",
      "default": "./src/types.ts"
    }
  },
  "main": "./src/index.ts",
  "types": "./src/index.ts",
  "files": ["dist"],
  "scripts": {
    "build": "pnpm build:types && pnpm build:swc",
    "build:swc": "swc ./src -d ./dist --config-file .swcrc --strip-leading-paths",
    "build:types": "tsc --emitDeclarationOnly --outDir dist",
    "clean": "rimraf -g {dist,*.tsbuildinfo}",
    "lint": "eslint .",
    "lint:fix": "eslint . --fix"
  },
  "dependencies": {},
  "devDependencies": {
    "@payloadcms/eslint-config": "workspace:*",
    "payload": "workspace:*"
  },
  "peerDependencies": {
    "@dotdo/db-clickhouse": "workspace:*",
    "payload": "workspace:*"
  },
  "publishConfig": {
    "exports": {
      ".": {
        "import": "./dist/index.js",
        "types": "./dist/index.d.ts",
        "default": "./dist/index.js"
      },
      "./types": {
        "import": "./dist/types.js",
        "types": "./dist/types.d.ts",
        "default": "./dist/types.js"
      }
    },
    "main": "./dist/index.js",
    "registry": "https://registry.npmjs.org/",
    "types": "./dist/index.d.ts"
  }
}
```

**Step 2: Create tsconfig.json**

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "outDir": "./dist",
    "rootDir": "./src"
  },
  "include": ["src/**/*"]
}
```

**Step 3: Create .swcrc**

```json
{
  "jsc": {
    "parser": {
      "syntax": "typescript",
      "tsx": false
    },
    "target": "es2022"
  },
  "module": {
    "type": "es6"
  },
  "sourceMaps": true
}
```

**Step 4: Create placeholder index.ts**

```typescript
// packages/plugin-clickhouse/src/index.ts
export const clickhousePlugin = () => (config: any) => config
```

**Step 5: Run pnpm install**

Run: `pnpm install`
Expected: Dependencies installed, workspace links created

**Step 6: Verify build**

Run: `pnpm run build:plugin-clickhouse`
Expected: Build succeeds

**Step 7: Commit**

```bash
git add packages/plugin-clickhouse
git commit -m "feat(plugin-clickhouse): initialize package structure"
```

---

## Task 2: Type Definitions

**Files:**

- Create: `packages/plugin-clickhouse/src/types.ts`

**Step 1: Write type definitions**

```typescript
// packages/plugin-clickhouse/src/types.ts
import type { Access, CollectionConfig } from 'payload'

/**
 * Search collection configuration
 */
export interface SearchConfig {
  /** Override collection slug (default: 'search') */
  slug?: string
  /** Override admin group */
  adminGroup?: string
  /** Collections to index with field configuration */
  collections?: Record<string, SearchCollectionConfig>
  /** Index all collections with default field extraction */
  indexAll?: {
    enabled: boolean
    /** Default fields to extract text from */
    defaultFields?: string[]
  }
  /** Characters per chunk for long documents */
  chunkSize?: number
  /** Overlap between chunks */
  chunkOverlap?: number
  /** Custom access control */
  access?: {
    read?: Access
  }
  /** Collection config overrides */
  overrides?: Partial<Omit<CollectionConfig, 'slug' | 'fields'>>
}

export interface SearchCollectionConfig {
  /** Fields to extract text from (supports dot notation for nested) */
  fields: string[]
  /** Field to use as title in search results */
  titleField?: string
}

/**
 * Events collection configuration
 */
export interface EventsConfig {
  /** Override collection slug (default: 'events') */
  slug?: string
  /** Override admin group */
  adminGroup?: string
  /** Track CRUD operations (doc.create, doc.update, doc.delete) */
  trackCRUD?: boolean
  /** Track auth events (auth.login, auth.logout, auth.failed) */
  trackAuth?: boolean
  /** Per-collection tracking configuration */
  collections?: Record<string, EventsCollectionConfig>
  /** Custom access control */
  access?: {
    read?: Access
  }
  /** Collection config overrides */
  overrides?: Partial<Omit<CollectionConfig, 'slug' | 'fields'>>
}

export interface EventsCollectionConfig {
  /** Enable tracking for this collection */
  track?: boolean
  /** Include input data in event log */
  includeInput?: boolean
}

/**
 * Relationships collection configuration
 */
export interface RelationshipsConfig {
  /** Override collection slug (default: 'relationships') */
  slug?: string
  /** Override admin group */
  adminGroup?: string
  /** Custom access control */
  access?: {
    read?: Access
  }
  /** Collection config overrides */
  overrides?: Partial<Omit<CollectionConfig, 'slug' | 'fields'>>
}

/**
 * Actions collection configuration
 */
export interface ActionsConfig {
  /** Override collection slug (default: 'actions') */
  slug?: string
  /** Override admin group */
  adminGroup?: string
  /** Retention policy for completed/failed actions */
  retention?: {
    /** Duration to keep completed actions (e.g., '30d') */
    completed?: string
    /** Duration to keep failed actions (e.g., '90d') */
    failed?: string
  }
  /** Custom access control */
  access?: {
    read?: Access
    update?: Access
    delete?: Access
  }
  /** Collection config overrides */
  overrides?: Partial<Omit<CollectionConfig, 'slug' | 'fields'>>
}

/**
 * Data collection configuration (optional debug view)
 */
export interface DataConfig {
  /** Override collection slug (default: 'data') */
  slug?: string
  /** Override admin group */
  adminGroup?: string
  /** Custom access control */
  access?: {
    read?: Access
  }
  /** Collection config overrides */
  overrides?: Partial<Omit<CollectionConfig, 'slug' | 'fields'>>
}

/**
 * Main plugin configuration
 */
export interface PluginClickHouseConfig {
  /** Default admin group for all collections */
  adminGroup?: string
  /** Search collection config (false to disable) */
  search?: false | SearchConfig
  /** Events collection config (false to disable) */
  events?: false | EventsConfig
  /** Relationships collection config (false to disable) */
  relationships?: false | RelationshipsConfig
  /** Actions collection config (false to disable) */
  actions?: false | ActionsConfig
  /** Data collection config (false to disable, disabled by default) */
  data?: false | DataConfig
}

/**
 * Sanitized plugin configuration with defaults applied
 */
export interface SanitizedPluginConfig {
  adminGroup: string
  search: false | Required<SearchConfig>
  events: false | Required<EventsConfig>
  relationships: false | Required<RelationshipsConfig>
  actions: false | Required<ActionsConfig>
  data: false | Required<DataConfig>
}
```

**Step 2: Commit**

```bash
git add packages/plugin-clickhouse/src/types.ts
git commit -m "feat(plugin-clickhouse): add type definitions"
```

---

## Task 3: Search Collection Generator

**Files:**

- Create: `packages/plugin-clickhouse/src/collections/search.ts`

**Step 1: Write search collection generator**

```typescript
// packages/plugin-clickhouse/src/collections/search.ts
import type { CollectionConfig } from 'payload'

import type { SearchConfig } from '../types.js'

export const generateSearchCollection = (
  config: SearchConfig | undefined,
  defaultAdminGroup: string,
): CollectionConfig => {
  const slug = config?.slug ?? 'search'
  const adminGroup = config?.adminGroup ?? defaultAdminGroup

  return {
    slug,
    labels: {
      singular: 'Search Index',
      plural: 'Search Index',
    },
    admin: {
      group: adminGroup,
      useAsTitle: 'text',
      defaultColumns: ['collection', 'docId', 'status', 'updatedAt'],
      description:
        'Full-text and vector search index entries synced from ClickHouse.',
      ...(config?.overrides?.admin || {}),
    },
    access: {
      create: () => false,
      update: () => false,
      delete: () => false,
      read: config?.access?.read ?? (() => true),
    },
    fields: [
      {
        name: 'collection',
        type: 'text',
        required: true,
        admin: { readOnly: true },
      },
      {
        name: 'docId',
        type: 'text',
        required: true,
        admin: { readOnly: true },
      },
      {
        name: 'text',
        type: 'textarea',
        admin: { readOnly: true },
      },
      {
        name: 'chunkIndex',
        type: 'number',
        defaultValue: 0,
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'status',
        type: 'select',
        options: [
          { label: 'Pending', value: 'pending' },
          { label: 'Ready', value: 'ready' },
          { label: 'Failed', value: 'failed' },
        ],
        defaultValue: 'pending',
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'error',
        type: 'text',
        admin: {
          readOnly: true,
          condition: (data) => data?.status === 'failed',
        },
      },
    ],
    ...(config?.overrides || {}),
  }
}
```

**Step 2: Commit**

```bash
git add packages/plugin-clickhouse/src/collections/search.ts
git commit -m "feat(plugin-clickhouse): add search collection generator"
```

---

## Task 4: Events Collection Generator

**Files:**

- Create: `packages/plugin-clickhouse/src/collections/events.ts`

**Step 1: Write events collection generator**

```typescript
// packages/plugin-clickhouse/src/collections/events.ts
import type { CollectionConfig } from 'payload'

import type { EventsConfig } from '../types.js'

export const generateEventsCollection = (
  config: EventsConfig | undefined,
  defaultAdminGroup: string,
): CollectionConfig => {
  const slug = config?.slug ?? 'events'
  const adminGroup = config?.adminGroup ?? defaultAdminGroup

  return {
    slug,
    labels: {
      singular: 'Event',
      plural: 'Events',
    },
    admin: {
      group: adminGroup,
      useAsTitle: 'type',
      defaultColumns: ['type', 'collection', 'docId', 'userId', 'timestamp'],
      description:
        'Immutable audit log of system and custom events from ClickHouse.',
      ...(config?.overrides?.admin || {}),
    },
    access: {
      // Immutable - no create, update, or delete
      create: () => false,
      update: () => false,
      delete: () => false,
      read: config?.access?.read ?? (({ req }) => !!req.user),
    },
    fields: [
      {
        name: 'type',
        type: 'text',
        required: true,
        admin: { readOnly: true },
      },
      {
        name: 'collection',
        type: 'text',
        admin: { readOnly: true },
      },
      {
        name: 'docId',
        type: 'text',
        admin: { readOnly: true },
      },
      {
        name: 'userId',
        type: 'text',
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'sessionId',
        type: 'text',
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'ip',
        type: 'text',
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'input',
        type: 'json',
        admin: { readOnly: true },
      },
      {
        name: 'result',
        type: 'json',
        admin: { readOnly: true },
      },
      {
        name: 'duration',
        type: 'number',
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'timestamp',
        type: 'date',
        admin: {
          readOnly: true,
          date: { displayFormat: 'PPpp' },
        },
      },
    ],
    ...(config?.overrides || {}),
  }
}
```

**Step 2: Commit**

```bash
git add packages/plugin-clickhouse/src/collections/events.ts
git commit -m "feat(plugin-clickhouse): add events collection generator"
```

---

## Task 5: Relationships Collection Generator

**Files:**

- Create: `packages/plugin-clickhouse/src/collections/relationships.ts`

**Step 1: Write relationships collection generator**

```typescript
// packages/plugin-clickhouse/src/collections/relationships.ts
import type { CollectionConfig } from 'payload'

import type { RelationshipsConfig } from '../types.js'

export const generateRelationshipsCollection = (
  config: RelationshipsConfig | undefined,
  defaultAdminGroup: string,
): CollectionConfig => {
  const slug = config?.slug ?? 'relationships'
  const adminGroup = config?.adminGroup ?? defaultAdminGroup

  return {
    slug,
    labels: {
      singular: 'Relationship',
      plural: 'Relationships',
    },
    admin: {
      group: adminGroup,
      defaultColumns: ['fromType', 'fromId', 'fromField', 'toType', 'toId'],
      description:
        'Document relationship graph from ClickHouse. Shows how documents link to each other.',
      ...(config?.overrides?.admin || {}),
    },
    access: {
      // Read-only - managed by db-adapter
      create: () => false,
      update: () => false,
      delete: () => false,
      read: config?.access?.read ?? (({ req }) => !!req.user),
    },
    fields: [
      {
        name: 'fromType',
        type: 'text',
        required: true,
        admin: { readOnly: true },
      },
      {
        name: 'fromId',
        type: 'text',
        required: true,
        admin: { readOnly: true },
      },
      {
        name: 'fromField',
        type: 'text',
        required: true,
        admin: { readOnly: true },
      },
      {
        name: 'toType',
        type: 'text',
        required: true,
        admin: { readOnly: true },
      },
      {
        name: 'toId',
        type: 'text',
        required: true,
        admin: { readOnly: true },
      },
      {
        name: 'position',
        type: 'number',
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'locale',
        type: 'text',
        admin: { readOnly: true, position: 'sidebar' },
      },
    ],
    ...(config?.overrides || {}),
  }
}
```

**Step 2: Commit**

```bash
git add packages/plugin-clickhouse/src/collections/relationships.ts
git commit -m "feat(plugin-clickhouse): add relationships collection generator"
```

---

## Task 6: Actions Collection Generator

**Files:**

- Create: `packages/plugin-clickhouse/src/collections/actions.ts`

**Step 1: Write actions collection generator**

```typescript
// packages/plugin-clickhouse/src/collections/actions.ts
import type { CollectionConfig } from 'payload'

import type { ActionsConfig } from '../types.js'

export const generateActionsCollection = (
  config: ActionsConfig | undefined,
  defaultAdminGroup: string,
): CollectionConfig => {
  const slug = config?.slug ?? 'actions'
  const adminGroup = config?.adminGroup ?? defaultAdminGroup

  return {
    slug,
    labels: {
      singular: 'Action',
      plural: 'Actions',
    },
    admin: {
      group: adminGroup,
      useAsTitle: 'name',
      defaultColumns: ['name', 'type', 'status', 'collection', 'createdAt'],
      description:
        'Task and job orchestration queue from ClickHouse. Monitor background jobs, workflows, and human-in-the-loop tasks.',
      ...(config?.overrides?.admin || {}),
    },
    access: {
      create: () => false, // Created via API only
      update: config?.access?.update ?? (({ req }) => !!req.user),
      delete: config?.access?.delete ?? (({ req }) => !!req.user),
      read: config?.access?.read ?? (({ req }) => !!req.user),
    },
    fields: [
      // Identity
      {
        name: 'type',
        type: 'select',
        required: true,
        options: [
          { label: 'Transaction', value: 'transaction' },
          { label: 'Job', value: 'job' },
          { label: 'Workflow', value: 'workflow' },
          { label: 'Task', value: 'task' },
        ],
        admin: { readOnly: true },
      },
      {
        name: 'name',
        type: 'text',
        required: true,
        admin: { readOnly: true },
      },
      // Status
      {
        name: 'status',
        type: 'select',
        required: true,
        options: [
          { label: 'Pending', value: 'pending' },
          { label: 'Running', value: 'running' },
          { label: 'Waiting', value: 'waiting' },
          { label: 'Completed', value: 'completed' },
          { label: 'Failed', value: 'failed' },
          { label: 'Cancelled', value: 'cancelled' },
        ],
        defaultValue: 'pending',
        admin: { position: 'sidebar' },
      },
      {
        name: 'priority',
        type: 'number',
        defaultValue: 0,
        admin: { position: 'sidebar' },
      },
      // Context
      {
        name: 'collection',
        type: 'text',
        admin: { readOnly: true },
      },
      {
        name: 'docId',
        type: 'text',
        admin: { readOnly: true },
      },
      // Payload
      {
        name: 'input',
        type: 'json',
        admin: { readOnly: true },
      },
      {
        name: 'output',
        type: 'json',
        admin: { readOnly: true },
      },
      {
        name: 'error',
        type: 'json',
        admin: {
          readOnly: true,
          condition: (data) => data?.status === 'failed',
        },
      },
      // Workflow state
      {
        name: 'step',
        type: 'number',
        defaultValue: 0,
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'steps',
        type: 'json',
        admin: { readOnly: true },
      },
      {
        name: 'context',
        type: 'json',
        admin: { readOnly: true },
      },
      // Human-in-the-loop
      {
        name: 'assignedTo',
        type: 'text',
        admin: { position: 'sidebar' },
      },
      {
        name: 'waitingFor',
        type: 'select',
        options: [
          { label: 'Input', value: 'input' },
          { label: 'Approval', value: 'approval' },
          { label: 'Review', value: 'review' },
          { label: 'External', value: 'external' },
        ],
        admin: {
          position: 'sidebar',
          condition: (data) => data?.status === 'waiting',
        },
      },
      // Timing
      {
        name: 'scheduledAt',
        type: 'date',
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'startedAt',
        type: 'date',
        admin: { readOnly: true },
      },
      {
        name: 'completedAt',
        type: 'date',
        admin: { readOnly: true },
      },
      {
        name: 'timeoutAt',
        type: 'date',
        admin: { readOnly: true, position: 'sidebar' },
      },
      // Retry
      {
        name: 'attempts',
        type: 'number',
        defaultValue: 0,
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'maxAttempts',
        type: 'number',
        defaultValue: 3,
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'retryAfter',
        type: 'date',
        admin: { readOnly: true },
      },
      // Lineage
      {
        name: 'parentId',
        type: 'text',
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'rootId',
        type: 'text',
        admin: { readOnly: true, position: 'sidebar' },
      },
    ],
    ...(config?.overrides || {}),
  }
}
```

**Step 2: Commit**

```bash
git add packages/plugin-clickhouse/src/collections/actions.ts
git commit -m "feat(plugin-clickhouse): add actions collection generator"
```

---

## Task 7: Data Collection Generator (Optional Debug)

**Files:**

- Create: `packages/plugin-clickhouse/src/collections/data.ts`

**Step 1: Write data collection generator**

```typescript
// packages/plugin-clickhouse/src/collections/data.ts
import type { CollectionConfig } from 'payload'

import type { DataConfig } from '../types.js'

export const generateDataCollection = (
  config: DataConfig | undefined,
  defaultAdminGroup: string,
): CollectionConfig => {
  const slug = config?.slug ?? 'data'
  const adminGroup = config?.adminGroup ?? defaultAdminGroup

  return {
    slug,
    labels: {
      singular: 'Data Record',
      plural: 'Data Records',
    },
    admin: {
      group: adminGroup,
      useAsTitle: 'title',
      defaultColumns: ['type', 'title', 'id', 'updatedAt'],
      description:
        'Raw document data from ClickHouse. Debug view of the underlying data table.',
      ...(config?.overrides?.admin || {}),
    },
    access: {
      // Read-only debug view
      create: () => false,
      update: () => false,
      delete: () => false,
      read: config?.access?.read ?? (({ req }) => req.user?.role === 'admin'),
    },
    fields: [
      {
        name: 'type',
        type: 'text',
        required: true,
        admin: { readOnly: true },
      },
      {
        name: 'title',
        type: 'text',
        admin: { readOnly: true },
      },
      {
        name: 'data',
        type: 'json',
        admin: { readOnly: true },
      },
      {
        name: 'createdBy',
        type: 'text',
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'updatedBy',
        type: 'text',
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'deletedAt',
        type: 'date',
        admin: { readOnly: true, position: 'sidebar' },
      },
      {
        name: 'deletedBy',
        type: 'text',
        admin: { readOnly: true, position: 'sidebar' },
      },
    ],
    ...(config?.overrides || {}),
  }
}
```

**Step 2: Create collections index**

```typescript
// packages/plugin-clickhouse/src/collections/index.ts
export { generateActionsCollection } from './actions.js'
export { generateDataCollection } from './data.js'
export { generateEventsCollection } from './events.js'
export { generateRelationshipsCollection } from './relationships.js'
export { generateSearchCollection } from './search.js'
```

**Step 3: Commit**

```bash
git add packages/plugin-clickhouse/src/collections/
git commit -m "feat(plugin-clickhouse): add data collection generator and index"
```

---

## Task 8: Plugin Entry Point

**Files:**

- Modify: `packages/plugin-clickhouse/src/index.ts`

**Step 1: Write main plugin function**

````typescript
// packages/plugin-clickhouse/src/index.ts
import type { Config, Plugin } from 'payload'

import type { PluginClickHouseConfig } from './types.js'

import {
  generateActionsCollection,
  generateDataCollection,
  generateEventsCollection,
  generateRelationshipsCollection,
  generateSearchCollection,
} from './collections/index.js'

export type { PluginClickHouseConfig } from './types.js'

const DEFAULT_ADMIN_GROUP = 'ClickHouse'

/**
 * ClickHouse Plugin for Payload CMS
 *
 * Exposes ClickHouse tables as browsable collections in the admin UI
 * with automatic hooks for search indexing and event tracking.
 *
 * @example
 * ```typescript
 * import { clickhousePlugin } from '@dotdo/plugin-clickhouse'
 *
 * export default buildConfig({
 *   plugins: [
 *     clickhousePlugin({
 *       adminGroup: 'Analytics',
 *       search: {
 *         collections: {
 *           posts: { fields: ['title', 'content'] },
 *         },
 *       },
 *       events: { trackCRUD: true, trackAuth: true },
 *       relationships: true,
 *       actions: true,
 *     }),
 *   ],
 * })
 * ```
 */
export const clickhousePlugin =
  (pluginConfig: PluginClickHouseConfig = {}): Plugin =>
  (incomingConfig: Config): Config => {
    const adminGroup = pluginConfig.adminGroup ?? DEFAULT_ADMIN_GROUP
    const collections = [...(incomingConfig.collections || [])]

    // Add search collection
    if (pluginConfig.search !== false) {
      const searchConfig =
        pluginConfig.search === true ? {} : pluginConfig.search
      collections.push(generateSearchCollection(searchConfig, adminGroup))
    }

    // Add events collection
    if (pluginConfig.events !== false) {
      const eventsConfig =
        pluginConfig.events === true ? {} : pluginConfig.events
      collections.push(generateEventsCollection(eventsConfig, adminGroup))
    }

    // Add relationships collection
    if (pluginConfig.relationships !== false) {
      const relationshipsConfig =
        pluginConfig.relationships === true ? {} : pluginConfig.relationships
      collections.push(
        generateRelationshipsCollection(relationshipsConfig, adminGroup),
      )
    }

    // Add actions collection
    if (pluginConfig.actions !== false) {
      const actionsConfig =
        pluginConfig.actions === true ? {} : pluginConfig.actions
      collections.push(generateActionsCollection(actionsConfig, adminGroup))
    }

    // Add data collection (disabled by default)
    if (pluginConfig.data) {
      const dataConfig = pluginConfig.data === true ? {} : pluginConfig.data
      collections.push(generateDataCollection(dataConfig, adminGroup))
    }

    // TODO: Add hooks for search indexing (Task 9)
    // TODO: Add hooks for event tracking (Task 10)

    return {
      ...incomingConfig,
      collections,
    }
  }
````

**Step 2: Commit**

```bash
git add packages/plugin-clickhouse/src/index.ts
git commit -m "feat(plugin-clickhouse): add main plugin entry point"
```

---

## Task 9: Search Hooks

**Files:**

- Create: `packages/plugin-clickhouse/src/hooks/syncWithSearch.ts`
- Create: `packages/plugin-clickhouse/src/hooks/deleteFromSearch.ts`
- Modify: `packages/plugin-clickhouse/src/index.ts`

**Step 1: Write syncWithSearch hook**

```typescript
// packages/plugin-clickhouse/src/hooks/syncWithSearch.ts
import type { CollectionAfterChangeHook } from 'payload'

import type { SearchCollectionConfig } from '../types.js'

interface SyncWithSearchArgs {
  collectionSlug: string
  searchConfig: SearchCollectionConfig
  chunkSize?: number
  chunkOverlap?: number
}

/**
 * Extract text from a document based on configured fields
 */
const extractText = (
  doc: Record<string, unknown>,
  fields: string[],
): string => {
  const parts: string[] = []

  for (const fieldPath of fields) {
    const value = fieldPath.split('.').reduce<unknown>((obj, key) => {
      if (obj && typeof obj === 'object' && key in obj) {
        return (obj as Record<string, unknown>)[key]
      }
      return undefined
    }, doc)

    if (typeof value === 'string' && value.trim()) {
      parts.push(value.trim())
    }
  }

  return parts.join('\n\n')
}

/**
 * Chunk text into smaller pieces for embedding
 */
const chunkText = (
  text: string,
  chunkSize: number,
  overlap: number,
): string[] => {
  if (text.length <= chunkSize) {
    return [text]
  }

  const chunks: string[] = []
  let start = 0

  while (start < text.length) {
    const end = Math.min(start + chunkSize, text.length)
    chunks.push(text.slice(start, end))
    start = end - overlap
    if (start >= text.length - overlap) break
  }

  return chunks
}

export const syncWithSearch =
  ({
    collectionSlug,
    searchConfig,
    chunkSize = 1000,
    chunkOverlap = 100,
  }: SyncWithSearchArgs): CollectionAfterChangeHook =>
  async ({ doc, req }) => {
    const { payload } = req

    // Check if db adapter has syncToSearch
    if (typeof payload.db.syncToSearch !== 'function') {
      payload.logger.warn(
        'syncToSearch not available on database adapter - skipping search sync',
      )
      return doc
    }

    try {
      const text = extractText(doc, searchConfig.fields)

      if (!text) {
        return doc
      }

      const chunks = chunkText(text, chunkSize, chunkOverlap)

      for (let i = 0; i < chunks.length; i++) {
        await payload.db.syncToSearch({
          collection: collectionSlug,
          doc: {
            ...doc,
            _extractedText: chunks[i],
          },
          chunkIndex: i,
        })
      }
    } catch (error) {
      payload.logger.error({
        err: error,
        msg: `Failed to sync ${collectionSlug}/${doc.id} to search`,
      })
    }

    return doc
  }
```

**Step 2: Write deleteFromSearch hook**

```typescript
// packages/plugin-clickhouse/src/hooks/deleteFromSearch.ts
import type { CollectionBeforeDeleteHook } from 'payload'

interface DeleteFromSearchArgs {
  collectionSlug: string
}

export const deleteFromSearch =
  ({ collectionSlug }: DeleteFromSearchArgs): CollectionBeforeDeleteHook =>
  async ({ id, req }) => {
    const { payload } = req

    // Check if db adapter has search delete capability
    if (typeof payload.db.execute !== 'function') {
      return
    }

    try {
      // Delete all search entries for this document
      await payload.db.execute({
        query: `
          ALTER TABLE search
          DELETE WHERE collection = {collection:String} AND docId = {docId:String} AND ns = {ns:String}
        `,
        query_params: {
          collection: collectionSlug,
          docId: String(id),
          ns: payload.db.namespace,
        },
      })
    } catch (error) {
      payload.logger.error({
        err: error,
        msg: `Failed to delete ${collectionSlug}/${id} from search`,
      })
    }
  }
```

**Step 3: Create hooks index**

```typescript
// packages/plugin-clickhouse/src/hooks/index.ts
export { deleteFromSearch } from './deleteFromSearch.js'
export { syncWithSearch } from './syncWithSearch.js'
```

**Step 4: Update plugin to inject search hooks**

Update `packages/plugin-clickhouse/src/index.ts` to inject hooks into tracked collections:

```typescript
// Add after the collections array is built, before return statement:

// Inject search hooks into tracked collections
if (pluginConfig.search !== false && pluginConfig.search?.collections) {
  const searchConfig = pluginConfig.search
  const trackedCollections = Object.keys(searchConfig.collections)

  for (let i = 0; i < collections.length; i++) {
    const collection = collections[i]
    if (trackedCollections.includes(collection.slug)) {
      const collectionSearchConfig = searchConfig.collections[collection.slug]
      collections[i] = {
        ...collection,
        hooks: {
          ...collection.hooks,
          afterChange: [
            ...(collection.hooks?.afterChange || []),
            syncWithSearch({
              collectionSlug: collection.slug,
              searchConfig: collectionSearchConfig,
              chunkSize: searchConfig.chunkSize,
              chunkOverlap: searchConfig.chunkOverlap,
            }),
          ],
          beforeDelete: [
            ...(collection.hooks?.beforeDelete || []),
            deleteFromSearch({ collectionSlug: collection.slug }),
          ],
        },
      }
    }
  }
}
```

**Step 5: Commit**

```bash
git add packages/plugin-clickhouse/src/hooks/
git add packages/plugin-clickhouse/src/index.ts
git commit -m "feat(plugin-clickhouse): add search sync hooks"
```

---

## Task 10: Event Tracking Hooks

**Files:**

- Create: `packages/plugin-clickhouse/src/hooks/trackEvent.ts`
- Modify: `packages/plugin-clickhouse/src/index.ts`

**Step 1: Write trackEvent hook factory**

```typescript
// packages/plugin-clickhouse/src/hooks/trackEvent.ts
import type {
  CollectionAfterChangeHook,
  CollectionAfterDeleteHook,
} from 'payload'

import type { EventsCollectionConfig } from '../types.js'

interface TrackEventArgs {
  collectionSlug: string
  eventConfig?: EventsCollectionConfig
}

export const trackAfterChange =
  ({
    collectionSlug,
    eventConfig,
  }: TrackEventArgs): CollectionAfterChangeHook =>
  async ({ doc, operation, previousDoc, req }) => {
    const { payload } = req

    if (typeof payload.db.logEvent !== 'function') {
      return doc
    }

    const eventType = operation === 'create' ? 'doc.create' : 'doc.update'
    const includeInput = eventConfig?.includeInput !== false

    try {
      await payload.db.logEvent({
        type: eventType,
        collection: collectionSlug,
        docId: String(doc.id),
        userId: req.user?.id ? String(req.user.id) : undefined,
        sessionId: req.headers?.get?.('x-session-id') || undefined,
        ip:
          req.headers?.get?.('x-forwarded-for') ||
          req.headers?.get?.('x-real-ip') ||
          undefined,
        input: includeInput
          ? { operation, hasChanges: !!previousDoc }
          : undefined,
        result: { success: true },
      })
    } catch (error) {
      payload.logger.error({
        err: error,
        msg: `Failed to log ${eventType} event for ${collectionSlug}/${doc.id}`,
      })
    }

    return doc
  }

export const trackAfterDelete =
  ({
    collectionSlug,
    eventConfig,
  }: TrackEventArgs): CollectionAfterDeleteHook =>
  async ({ doc, id, req }) => {
    const { payload } = req

    if (typeof payload.db.logEvent !== 'function') {
      return doc
    }

    try {
      await payload.db.logEvent({
        type: 'doc.delete',
        collection: collectionSlug,
        docId: String(id),
        userId: req.user?.id ? String(req.user.id) : undefined,
        sessionId: req.headers?.get?.('x-session-id') || undefined,
        ip:
          req.headers?.get?.('x-forwarded-for') ||
          req.headers?.get?.('x-real-ip') ||
          undefined,
        result: { success: true },
      })
    } catch (error) {
      payload.logger.error({
        err: error,
        msg: `Failed to log doc.delete event for ${collectionSlug}/${id}`,
      })
    }

    return doc
  }
```

**Step 2: Update hooks index**

```typescript
// packages/plugin-clickhouse/src/hooks/index.ts
export { deleteFromSearch } from './deleteFromSearch.js'
export { syncWithSearch } from './syncWithSearch.js'
export { trackAfterChange, trackAfterDelete } from './trackEvent.js'
```

**Step 3: Update plugin to inject event hooks**

Add to `packages/plugin-clickhouse/src/index.ts` after search hooks injection:

```typescript
// Inject event tracking hooks into collections
if (pluginConfig.events !== false) {
  const eventsConfig = pluginConfig.events === true ? {} : pluginConfig.events

  if (eventsConfig.trackCRUD !== false) {
    for (let i = 0; i < collections.length; i++) {
      const collection = collections[i]

      // Skip plugin-generated collections
      const pluginSlugs = [
        'search',
        'events',
        'relationships',
        'actions',
        'data',
      ]
      if (pluginSlugs.includes(collection.slug)) continue

      // Check collection-specific config
      const collectionConfig = eventsConfig.collections?.[collection.slug]
      if (collectionConfig?.track === false) continue

      collections[i] = {
        ...collection,
        hooks: {
          ...collection.hooks,
          afterChange: [
            ...(collection.hooks?.afterChange || []),
            trackAfterChange({
              collectionSlug: collection.slug,
              eventConfig: collectionConfig,
            }),
          ],
          afterDelete: [
            ...(collection.hooks?.afterDelete || []),
            trackAfterDelete({
              collectionSlug: collection.slug,
              eventConfig: collectionConfig,
            }),
          ],
        },
      }
    }
  }
}
```

**Step 4: Commit**

```bash
git add packages/plugin-clickhouse/src/hooks/
git add packages/plugin-clickhouse/src/index.ts
git commit -m "feat(plugin-clickhouse): add event tracking hooks"
```

---

## Task 11: Track API Helper

**Files:**

- Create: `packages/plugin-clickhouse/src/utilities/track.ts`
- Modify: `packages/plugin-clickhouse/src/index.ts`

**Step 1: Write track utility**

````typescript
// packages/plugin-clickhouse/src/utilities/track.ts
import type { Payload } from 'payload'

export interface TrackArgs {
  collection?: string
  docId?: string
  duration?: number
  input?: Record<string, unknown>
  ip?: string
  result?: Record<string, unknown>
  sessionId?: string
  userId?: string
}

/**
 * Track a custom event to the ClickHouse events table
 *
 * @example
 * ```typescript
 * await payload.db.track('checkout.completed', {
 *   input: { orderId: '123', total: 99.99 },
 * })
 * ```
 */
export const createTrackFunction = (payload: Payload) => {
  return async (
    type: string,
    args: TrackArgs = {},
  ): Promise<string | undefined> => {
    if (typeof payload.db.logEvent !== 'function') {
      payload.logger.warn(
        'logEvent not available on database adapter - track() is a no-op',
      )
      return undefined
    }

    return payload.db.logEvent({
      type,
      collection: args.collection,
      docId: args.docId,
      userId: args.userId,
      sessionId: args.sessionId,
      ip: args.ip,
      input: args.input,
      result: args.result,
      duration: args.duration,
    })
  }
}
````

**Step 2: Commit**

```bash
git add packages/plugin-clickhouse/src/utilities/track.ts
git commit -m "feat(plugin-clickhouse): add track() utility function"
```

---

## Task 12: Relationship Query Helpers

**Files:**

- Create: `packages/plugin-clickhouse/src/utilities/relationships.ts`

**Step 1: Write relationship query utilities**

```typescript
// packages/plugin-clickhouse/src/utilities/relationships.ts
import type { Payload } from 'payload'

export interface GetLinksArgs {
  collection: string
  id: string
}

export interface LinkResult {
  fromType: string
  fromId: string
  fromField: string
  toType: string
  toId: string
  position: number
  locale: string | null
}

export interface TraverseGraphArgs {
  collection: string
  id: string
  depth?: number
  direction?: 'both' | 'incoming' | 'outgoing'
}

export interface GraphNode {
  collection: string
  id: string
  depth: number
  links: LinkResult[]
}

/**
 * Find all documents that link TO a specific document
 */
export const createGetIncomingLinks = (payload: Payload) => {
  return async ({ collection, id }: GetLinksArgs): Promise<LinkResult[]> => {
    if (typeof payload.db.execute !== 'function') {
      return []
    }

    const results = await payload.db.execute<LinkResult>({
      query: `
        SELECT fromType, fromId, fromField, toType, toId, position, locale
        FROM relationships
        WHERE toType = {toType:String}
          AND toId = {toId:String}
          AND ns = {ns:String}
          AND deletedAt IS NULL
        ORDER BY fromType, fromId
      `,
      query_params: {
        toType: collection,
        toId: id,
        ns: payload.db.namespace,
      },
    })

    return results
  }
}

/**
 * Find all documents that a specific document links TO
 */
export const createGetOutgoingLinks = (payload: Payload) => {
  return async ({ collection, id }: GetLinksArgs): Promise<LinkResult[]> => {
    if (typeof payload.db.execute !== 'function') {
      return []
    }

    const results = await payload.db.execute<LinkResult>({
      query: `
        SELECT fromType, fromId, fromField, toType, toId, position, locale
        FROM relationships
        WHERE fromType = {fromType:String}
          AND fromId = {fromId:String}
          AND ns = {ns:String}
          AND deletedAt IS NULL
        ORDER BY toType, toId
      `,
      query_params: {
        fromType: collection,
        fromId: id,
        ns: payload.db.namespace,
      },
    })

    return results
  }
}

/**
 * Find orphaned references (links to documents that no longer exist)
 */
export const createFindOrphanedLinks = (payload: Payload) => {
  return async (args?: { collection?: string }): Promise<LinkResult[]> => {
    if (typeof payload.db.execute !== 'function') {
      return []
    }

    const collectionFilter = args?.collection
      ? 'AND r.fromType = {collection:String}'
      : ''

    const results = await payload.db.execute<LinkResult>({
      query: `
        SELECT r.fromType, r.fromId, r.fromField, r.toType, r.toId, r.position, r.locale
        FROM relationships r
        LEFT JOIN data d ON r.toType = d.type AND r.toId = d.id AND r.ns = d.ns
        WHERE r.ns = {ns:String}
          AND r.deletedAt IS NULL
          AND (d.id IS NULL OR d.deletedAt IS NOT NULL)
          ${collectionFilter}
        ORDER BY r.fromType, r.fromId
      `,
      query_params: {
        ns: payload.db.namespace,
        collection: args?.collection || '',
      },
    })

    return results
  }
}

/**
 * Traverse the document graph to find connected documents
 */
export const createTraverseGraph = (payload: Payload) => {
  return async ({
    collection,
    id,
    depth = 2,
    direction = 'both',
  }: TraverseGraphArgs): Promise<GraphNode[]> => {
    const visited = new Set<string>()
    const results: GraphNode[] = []
    const getIncoming = createGetIncomingLinks(payload)
    const getOutgoing = createGetOutgoingLinks(payload)

    const traverse = async (
      col: string,
      docId: string,
      currentDepth: number,
    ) => {
      const key = `${col}:${docId}`
      if (visited.has(key) || currentDepth > depth) return
      visited.add(key)

      const links: LinkResult[] = []

      if (direction === 'incoming' || direction === 'both') {
        const incoming = await getIncoming({ collection: col, id: docId })
        links.push(...incoming)
      }

      if (direction === 'outgoing' || direction === 'both') {
        const outgoing = await getOutgoing({ collection: col, id: docId })
        links.push(...outgoing)
      }

      results.push({
        collection: col,
        id: docId,
        depth: currentDepth,
        links,
      })

      // Continue traversal
      for (const link of links) {
        if (direction === 'incoming' || direction === 'both') {
          await traverse(link.fromType, link.fromId, currentDepth + 1)
        }
        if (direction === 'outgoing' || direction === 'both') {
          await traverse(link.toType, link.toId, currentDepth + 1)
        }
      }
    }

    await traverse(collection, id, 0)
    return results
  }
}
```

**Step 2: Commit**

```bash
git add packages/plugin-clickhouse/src/utilities/relationships.ts
git commit -m "feat(plugin-clickhouse): add relationship query helpers"
```

---

## Task 13: Actions Queue API Helpers

**Files:**

- Create: `packages/plugin-clickhouse/src/utilities/actions.ts`
- Create: `packages/plugin-clickhouse/src/utilities/index.ts`

**Step 1: Write actions queue utilities**

```typescript
// packages/plugin-clickhouse/src/utilities/actions.ts
import type { Payload } from 'payload'

export interface EnqueueArgs {
  name: string
  type?: 'job' | 'task' | 'transaction' | 'workflow'
  collection?: string
  docId?: string
  input?: Record<string, unknown>
  priority?: number
  scheduledAt?: Date
  assignedTo?: string
  waitingFor?: 'approval' | 'external' | 'input' | 'review'
  steps?: Array<{
    name: string
    handler: string
    input?: Record<string, unknown>
    waitingFor?: 'approval' | 'external' | 'input' | 'review'
  }>
  maxAttempts?: number
  timeoutMs?: number
  parentId?: string
  rootId?: string
}

export interface ActionRecord {
  id: string
  type: string
  name: string
  status: string
  priority: number
  collection: string | null
  docId: string | null
  input: Record<string, unknown>
  output: Record<string, unknown>
  error: Record<string, unknown> | null
  step: number
  steps: unknown[]
  context: Record<string, unknown>
  assignedTo: string | null
  waitingFor: string | null
  scheduledAt: string | null
  startedAt: string | null
  completedAt: string | null
  timeoutAt: string | null
  attempts: number
  maxAttempts: number
  retryAfter: string | null
  parentId: string | null
  rootId: string | null
  createdAt: string
  updatedAt: string
}

export interface ClaimActionsArgs {
  name?: string
  type?: string
  limit?: number
  lockFor?: number // milliseconds
}

export interface CompleteActionArgs {
  id: string
  output?: Record<string, unknown>
}

export interface FailActionArgs {
  id: string
  error: Record<string, unknown>
  retryAfter?: Date
}

export interface ResumeActionArgs {
  id: string
  input?: Record<string, unknown>
}

/**
 * Enqueue a new action/job
 */
export const createEnqueue = (payload: Payload) => {
  return async (args: EnqueueArgs): Promise<string> => {
    if (typeof payload.db.execute !== 'function') {
      throw new Error('execute not available on database adapter')
    }

    const id =
      payload.db.idType === 'uuid'
        ? crypto.randomUUID()
        : (await import('nanoid')).nanoid()

    const now = Date.now()
    const timeoutAt = args.timeoutMs ? new Date(now + args.timeoutMs) : null

    await payload.db.execute({
      query: `
        INSERT INTO actions (
          id, ns, type, name, status, priority,
          collection, docId, input, output, error,
          step, steps, context,
          assignedTo, waitingFor,
          scheduledAt, startedAt, completedAt, timeoutAt,
          attempts, maxAttempts, retryAfter,
          parentId, rootId,
          createdAt, updatedAt, v
        ) VALUES (
          {id:String}, {ns:String}, {type:String}, {name:String}, {status:String}, {priority:Int32},
          {collection:Nullable(String)}, {docId:Nullable(String)}, {input:String}, {output:String}, {error:Nullable(String)},
          {step:Int32}, {steps:String}, {context:String},
          {assignedTo:Nullable(String)}, {waitingFor:Nullable(String)},
          {scheduledAt:Nullable(DateTime64(3))}, {startedAt:Nullable(DateTime64(3))}, {completedAt:Nullable(DateTime64(3))}, {timeoutAt:Nullable(DateTime64(3))},
          {attempts:Int32}, {maxAttempts:Int32}, {retryAfter:Nullable(DateTime64(3))},
          {parentId:Nullable(String)}, {rootId:Nullable(String)},
          {createdAt:DateTime64(3)}, {updatedAt:DateTime64(3)}, {v:DateTime64(3)}
        )
      `,
      query_params: {
        id,
        ns: payload.db.namespace,
        type: args.type || 'job',
        name: args.name,
        status: args.scheduledAt ? 'pending' : 'pending',
        priority: args.priority || 0,
        collection: args.collection || null,
        docId: args.docId || null,
        input: JSON.stringify(args.input || {}),
        output: JSON.stringify({}),
        error: null,
        step: 0,
        steps: JSON.stringify(args.steps || []),
        context: JSON.stringify({}),
        assignedTo: args.assignedTo || null,
        waitingFor: args.waitingFor || null,
        scheduledAt: args.scheduledAt || null,
        startedAt: null,
        completedAt: null,
        timeoutAt,
        attempts: 0,
        maxAttempts: args.maxAttempts || 3,
        retryAfter: null,
        parentId: args.parentId || null,
        rootId: args.rootId || null,
        createdAt: now,
        updatedAt: now,
        v: now,
      },
    })

    return id
  }
}

/**
 * Enqueue multiple actions in a batch
 */
export const createEnqueueBatch = (payload: Payload) => {
  const enqueue = createEnqueue(payload)

  return async (actions: EnqueueArgs[]): Promise<string[]> => {
    const ids: string[] = []
    for (const action of actions) {
      const id = await enqueue(action)
      ids.push(id)
    }
    return ids
  }
}

/**
 * Claim actions for processing (atomic operation)
 */
export const createClaimActions = (payload: Payload) => {
  return async ({
    name,
    type,
    limit = 10,
    lockFor = 60000,
  }: ClaimActionsArgs): Promise<ActionRecord[]> => {
    if (typeof payload.db.execute !== 'function') {
      return []
    }

    const now = Date.now()
    const lockUntil = new Date(now + lockFor)

    // Find and claim pending actions
    const nameFilter = name ? 'AND name = {name:String}' : ''
    const typeFilter = type ? 'AND type = {type:String}' : ''

    const actions = await payload.db.execute<ActionRecord>({
      query: `
        SELECT *
        FROM actions
        WHERE ns = {ns:String}
          AND status = 'pending'
          AND (scheduledAt IS NULL OR scheduledAt <= {now:DateTime64(3)})
          AND (retryAfter IS NULL OR retryAfter <= {now:DateTime64(3)})
          ${nameFilter}
          ${typeFilter}
        ORDER BY priority DESC, createdAt ASC
        LIMIT {limit:UInt32}
      `,
      query_params: {
        ns: payload.db.namespace,
        now,
        name: name || '',
        type: type || '',
        limit,
      },
    })

    if (actions.length === 0) return []

    // Mark as running
    const ids = actions.map((a) => a.id)
    await payload.db.execute({
      query: `
        ALTER TABLE actions
        UPDATE status = 'running', startedAt = {now:DateTime64(3)}, timeoutAt = {lockUntil:DateTime64(3)}, attempts = attempts + 1, v = {now:DateTime64(3)}
        WHERE id IN ({ids:Array(String)}) AND ns = {ns:String}
      `,
      query_params: {
        ns: payload.db.namespace,
        now,
        lockUntil,
        ids,
      },
    })

    return actions
  }
}

/**
 * Complete an action successfully
 */
export const createCompleteAction = (payload: Payload) => {
  return async ({ id, output = {} }: CompleteActionArgs): Promise<void> => {
    if (typeof payload.db.execute !== 'function') return

    const now = Date.now()
    await payload.db.execute({
      query: `
        ALTER TABLE actions
        UPDATE status = 'completed', output = {output:String}, completedAt = {now:DateTime64(3)}, v = {now:DateTime64(3)}
        WHERE id = {id:String} AND ns = {ns:String}
      `,
      query_params: {
        id,
        ns: payload.db.namespace,
        output: JSON.stringify(output),
        now,
      },
    })
  }
}

/**
 * Fail an action (will retry if attempts < maxAttempts)
 */
export const createFailAction = (payload: Payload) => {
  return async ({ id, error, retryAfter }: FailActionArgs): Promise<void> => {
    if (typeof payload.db.execute !== 'function') return

    const now = Date.now()

    // Check if should retry
    const [action] = await payload.db.execute<ActionRecord>({
      query: `SELECT attempts, maxAttempts FROM actions WHERE id = {id:String} AND ns = {ns:String}`,
      query_params: { id, ns: payload.db.namespace },
    })

    const shouldRetry = action && action.attempts < action.maxAttempts
    const newStatus = shouldRetry ? 'pending' : 'failed'

    await payload.db.execute({
      query: `
        ALTER TABLE actions
        UPDATE
          status = {status:String},
          error = {error:String},
          retryAfter = {retryAfter:Nullable(DateTime64(3))},
          completedAt = {completedAt:Nullable(DateTime64(3))},
          v = {now:DateTime64(3)}
        WHERE id = {id:String} AND ns = {ns:String}
      `,
      query_params: {
        id,
        ns: payload.db.namespace,
        status: newStatus,
        error: JSON.stringify(error),
        retryAfter: shouldRetry ? retryAfter || new Date(now + 60000) : null,
        completedAt: shouldRetry ? null : now,
        now,
      },
    })
  }
}

/**
 * Cancel an action
 */
export const createCancelAction = (payload: Payload) => {
  return async ({ id }: { id: string }): Promise<void> => {
    if (typeof payload.db.execute !== 'function') return

    const now = Date.now()
    await payload.db.execute({
      query: `
        ALTER TABLE actions
        UPDATE status = 'cancelled', completedAt = {now:DateTime64(3)}, v = {now:DateTime64(3)}
        WHERE id = {id:String} AND ns = {ns:String}
      `,
      query_params: { id, ns: payload.db.namespace, now },
    })
  }
}

/**
 * Resume an action that was waiting for input
 */
export const createResumeAction = (payload: Payload) => {
  return async ({ id, input = {} }: ResumeActionArgs): Promise<void> => {
    if (typeof payload.db.execute !== 'function') return

    const now = Date.now()

    // Get current action to merge context
    const [action] = await payload.db.execute<ActionRecord>({
      query: `SELECT context, step FROM actions WHERE id = {id:String} AND ns = {ns:String}`,
      query_params: { id, ns: payload.db.namespace },
    })

    const currentContext = action ? JSON.parse(String(action.context)) : {}
    const newContext = {
      ...currentContext,
      [`step_${action?.step}_input`]: input,
    }

    await payload.db.execute({
      query: `
        ALTER TABLE actions
        UPDATE
          status = 'pending',
          waitingFor = NULL,
          context = {context:String},
          step = step + 1,
          v = {now:DateTime64(3)}
        WHERE id = {id:String} AND ns = {ns:String}
      `,
      query_params: {
        id,
        ns: payload.db.namespace,
        context: JSON.stringify(newContext),
        now,
      },
    })
  }
}

/**
 * Get actions for a specific document
 */
export const createGetDocumentActions = (payload: Payload) => {
  return async ({
    collection,
    docId,
    status,
  }: {
    collection: string
    docId: string
    status?: string[]
  }): Promise<ActionRecord[]> => {
    if (typeof payload.db.execute !== 'function') return []

    const statusFilter = status?.length
      ? 'AND status IN ({status:Array(String)})'
      : ''

    return payload.db.execute<ActionRecord>({
      query: `
        SELECT *
        FROM actions
        WHERE collection = {collection:String}
          AND docId = {docId:String}
          AND ns = {ns:String}
          ${statusFilter}
        ORDER BY createdAt DESC
      `,
      query_params: {
        collection,
        docId,
        ns: payload.db.namespace,
        status: status || [],
      },
    })
  }
}

/**
 * Get tasks assigned to a user
 */
export const createGetAssignedTasks = (payload: Payload) => {
  return async ({
    userId,
    waitingFor,
  }: {
    userId: string
    waitingFor?: string
  }): Promise<ActionRecord[]> => {
    if (typeof payload.db.execute !== 'function') return []

    const waitingForFilter = waitingFor
      ? 'AND waitingFor = {waitingFor:String}'
      : ''

    return payload.db.execute<ActionRecord>({
      query: `
        SELECT *
        FROM actions
        WHERE assignedTo = {userId:String}
          AND status = 'waiting'
          AND ns = {ns:String}
          ${waitingForFilter}
        ORDER BY priority DESC, createdAt ASC
      `,
      query_params: {
        userId,
        ns: payload.db.namespace,
        waitingFor: waitingFor || '',
      },
    })
  }
}
```

**Step 2: Create utilities index**

```typescript
// packages/plugin-clickhouse/src/utilities/index.ts
export {
  createCancelAction,
  createClaimActions,
  createCompleteAction,
  createEnqueue,
  createEnqueueBatch,
  createFailAction,
  createGetAssignedTasks,
  createGetDocumentActions,
  createResumeAction,
} from './actions.js'
export {
  createFindOrphanedLinks,
  createGetIncomingLinks,
  createGetOutgoingLinks,
  createTraverseGraph,
} from './relationships.js'
export { createTrackFunction } from './track.js'
```

**Step 3: Commit**

```bash
git add packages/plugin-clickhouse/src/utilities/
git commit -m "feat(plugin-clickhouse): add actions queue API helpers"
```

---

## Task 14: Integration Test

**Files:**

- Create: `packages/plugin-clickhouse/src/__tests__/plugin.spec.ts`

**Step 1: Write integration test**

```typescript
// packages/plugin-clickhouse/src/__tests__/plugin.spec.ts
import type { Config } from 'payload'

import { clickhousePlugin } from '../index.js'

describe('clickhousePlugin', () => {
  const baseConfig: Config = {
    collections: [
      {
        slug: 'posts',
        fields: [
          { name: 'title', type: 'text' },
          { name: 'content', type: 'textarea' },
        ],
      },
      {
        slug: 'users',
        fields: [{ name: 'email', type: 'email' }],
        auth: true,
      },
    ],
    secret: 'test-secret',
  }

  it('should add all collections when no config provided', () => {
    const plugin = clickhousePlugin({})
    const result = plugin(baseConfig)

    expect(result.collections).toHaveLength(6) // 2 original + 4 plugin
    expect(result.collections?.map((c) => c.slug)).toContain('search')
    expect(result.collections?.map((c) => c.slug)).toContain('events')
    expect(result.collections?.map((c) => c.slug)).toContain('relationships')
    expect(result.collections?.map((c) => c.slug)).toContain('actions')
  })

  it('should allow disabling collections', () => {
    const plugin = clickhousePlugin({
      search: false,
      events: false,
    })
    const result = plugin(baseConfig)

    expect(result.collections).toHaveLength(4) // 2 original + 2 plugin
    expect(result.collections?.map((c) => c.slug)).not.toContain('search')
    expect(result.collections?.map((c) => c.slug)).not.toContain('events')
  })

  it('should use custom slugs', () => {
    const plugin = clickhousePlugin({
      search: { slug: 'search-index' },
      events: { slug: 'audit-log' },
      relationships: { slug: 'links' },
      actions: { slug: 'tasks' },
    })
    const result = plugin(baseConfig)

    expect(result.collections?.map((c) => c.slug)).toContain('search-index')
    expect(result.collections?.map((c) => c.slug)).toContain('audit-log')
    expect(result.collections?.map((c) => c.slug)).toContain('links')
    expect(result.collections?.map((c) => c.slug)).toContain('tasks')
  })

  it('should use custom admin group', () => {
    const plugin = clickhousePlugin({
      adminGroup: 'Analytics',
    })
    const result = plugin(baseConfig)

    const searchCollection = result.collections?.find(
      (c) => c.slug === 'search',
    )
    expect(searchCollection?.admin?.group).toBe('Analytics')
  })

  it('should allow per-collection admin group override', () => {
    const plugin = clickhousePlugin({
      adminGroup: 'Analytics',
      events: { adminGroup: 'Audit' },
    })
    const result = plugin(baseConfig)

    const searchCollection = result.collections?.find(
      (c) => c.slug === 'search',
    )
    const eventsCollection = result.collections?.find(
      (c) => c.slug === 'events',
    )

    expect(searchCollection?.admin?.group).toBe('Analytics')
    expect(eventsCollection?.admin?.group).toBe('Audit')
  })

  it('should add data collection when enabled', () => {
    const plugin = clickhousePlugin({
      data: { slug: 'raw-data' },
    })
    const result = plugin(baseConfig)

    expect(result.collections?.map((c) => c.slug)).toContain('raw-data')
  })

  it('should inject search hooks into tracked collections', () => {
    const plugin = clickhousePlugin({
      search: {
        collections: {
          posts: { fields: ['title', 'content'] },
        },
      },
    })
    const result = plugin(baseConfig)

    const postsCollection = result.collections?.find((c) => c.slug === 'posts')
    expect(postsCollection?.hooks?.afterChange).toHaveLength(1)
    expect(postsCollection?.hooks?.beforeDelete).toHaveLength(1)
  })

  it('should inject event hooks when trackCRUD is enabled', () => {
    const plugin = clickhousePlugin({
      events: { trackCRUD: true },
    })
    const result = plugin(baseConfig)

    const postsCollection = result.collections?.find((c) => c.slug === 'posts')
    expect(postsCollection?.hooks?.afterChange).toBeDefined()
    expect(postsCollection?.hooks?.afterDelete).toBeDefined()
  })

  it('should respect collection-level event tracking config', () => {
    const plugin = clickhousePlugin({
      events: {
        trackCRUD: true,
        collections: {
          users: { track: false },
        },
      },
    })
    const result = plugin(baseConfig)

    const postsCollection = result.collections?.find((c) => c.slug === 'posts')
    const usersCollection = result.collections?.find((c) => c.slug === 'users')

    expect(postsCollection?.hooks?.afterChange).toBeDefined()
    // Users should not have event hooks (or should have fewer)
    const usersAfterChangeCount =
      usersCollection?.hooks?.afterChange?.length || 0
    const postsAfterChangeCount =
      postsCollection?.hooks?.afterChange?.length || 0
    expect(usersAfterChangeCount).toBeLessThan(postsAfterChangeCount)
  })

  it('events collection should be immutable', () => {
    const plugin = clickhousePlugin({})
    const result = plugin(baseConfig)

    const eventsCollection = result.collections?.find(
      (c) => c.slug === 'events',
    )

    expect(eventsCollection?.access?.create).toBeDefined()
    expect(eventsCollection?.access?.update).toBeDefined()
    expect(eventsCollection?.access?.delete).toBeDefined()

    // All should return false
    const mockReq = { user: { id: '1', role: 'admin' } } as any
    expect(eventsCollection?.access?.create?.({ req: mockReq } as any)).toBe(
      false,
    )
    expect(eventsCollection?.access?.update?.({ req: mockReq } as any)).toBe(
      false,
    )
    expect(eventsCollection?.access?.delete?.({ req: mockReq } as any)).toBe(
      false,
    )
  })
})
```

**Step 2: Run tests**

Run: `pnpm test packages/plugin-clickhouse`
Expected: All tests pass

**Step 3: Commit**

```bash
git add packages/plugin-clickhouse/src/__tests__/
git commit -m "test(plugin-clickhouse): add unit tests for plugin"
```

---

## Final Checklist

- [ ] Package setup complete (Task 1)
- [ ] Type definitions in place (Task 2)
- [ ] All collection generators implemented (Tasks 3-7)
- [ ] Plugin entry point working (Task 8)
- [ ] Search hooks injecting correctly (Task 9)
- [ ] Event hooks injecting correctly (Task 10)
- [ ] track() API helper working (Task 11)
- [ ] Relationship query helpers working (Task 12)
- [ ] Actions queue API helpers working (Task 13)
- [ ] Tests passing (Task 14)
- [ ] Build succeeds: `pnpm run build:plugin-clickhouse`
