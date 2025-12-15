// packages/plugin-clickhouse/src/types.ts
import type { Access, CollectionConfig } from 'payload'

/**
 * Search collection configuration
 */
export interface SearchConfig {
  /** Custom access control */
  access?: {
    read?: Access
  }
  /** Override admin group */
  adminGroup?: string
  /** Overlap between chunks */
  chunkOverlap?: number
  /** Characters per chunk for long documents */
  chunkSize?: number
  /** Collections to index with field configuration */
  collections?: Record<string, SearchCollectionConfig>
  /** Index all collections with default field extraction */
  indexAll?: {
    /** Default fields to extract text from */
    defaultFields?: string[]
    enabled: boolean
  }
  /** Collection config overrides */
  overrides?: Partial<Omit<CollectionConfig, 'fields' | 'slug'>>
  /** Override collection slug (default: 'search') */
  slug?: string
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
  /** Custom access control */
  access?: {
    read?: Access
  }
  /** Override admin group */
  adminGroup?: string
  /** Per-collection tracking configuration */
  collections?: Record<string, EventsCollectionConfig>
  /** Collection config overrides */
  overrides?: Partial<Omit<CollectionConfig, 'fields' | 'slug'>>
  /** Override collection slug (default: 'events') */
  slug?: string
  /** Track auth events (auth.login, auth.logout, auth.failed) */
  trackAuth?: boolean
  /** Track CRUD operations (doc.create, doc.update, doc.delete) */
  trackCRUD?: boolean
}

export interface EventsCollectionConfig {
  /** Include input data in event log */
  includeInput?: boolean
  /** Enable tracking for this collection */
  track?: boolean
}

/**
 * Relationships collection configuration
 */
export interface RelationshipsConfig {
  /** Custom access control */
  access?: {
    read?: Access
  }
  /** Override admin group */
  adminGroup?: string
  /** Collection config overrides */
  overrides?: Partial<Omit<CollectionConfig, 'fields' | 'slug'>>
  /** Override collection slug (default: 'relationships') */
  slug?: string
}

/**
 * Actions collection configuration
 */
export interface ActionsConfig {
  /** Custom access control */
  access?: {
    delete?: Access
    read?: Access
    update?: Access
  }
  /** Override admin group */
  adminGroup?: string
  /** Collection config overrides */
  overrides?: Partial<Omit<CollectionConfig, 'fields' | 'slug'>>
  /** Retention policy for completed/failed actions */
  retention?: {
    /** Duration to keep completed actions (e.g., '30d') */
    completed?: string
    /** Duration to keep failed actions (e.g., '90d') */
    failed?: string
  }
  /** Override collection slug (default: 'actions') */
  slug?: string
}

/**
 * Data collection configuration (optional debug view)
 */
export interface DataConfig {
  /** Custom access control */
  access?: {
    read?: Access
  }
  /** Override admin group */
  adminGroup?: string
  /** Collection config overrides */
  overrides?: Partial<Omit<CollectionConfig, 'fields' | 'slug'>>
  /** Override collection slug (default: 'data') */
  slug?: string
}

/**
 * Main plugin configuration
 */
export interface PluginClickHouseConfig {
  /** Actions collection config (false to disable) */
  actions?: ActionsConfig | false
  /** Default admin group for all collections */
  adminGroup?: string
  /** Data collection config (false to disable, disabled by default) */
  data?: DataConfig | false
  /** Events collection config (false to disable) */
  events?: EventsConfig | false
  /** Relationships collection config (false to disable) */
  relationships?: false | RelationshipsConfig
  /** Search collection config (false to disable) */
  search?: false | SearchConfig
}

/**
 * Sanitized plugin configuration with defaults applied
 */
export interface SanitizedPluginConfig {
  actions: false | Required<ActionsConfig>
  adminGroup: string
  data: false | Required<DataConfig>
  events: false | Required<EventsConfig>
  relationships: false | Required<RelationshipsConfig>
  search: false | Required<SearchConfig>
}
