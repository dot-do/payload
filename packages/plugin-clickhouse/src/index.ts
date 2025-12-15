import type { Config, Plugin } from 'payload'

import type { PluginClickHouseConfig } from './types.js'

import {
  generateActionsCollection,
  generateDataCollection,
  generateEventsCollection,
  generateRelationshipsCollection,
  generateSearchCollection,
} from './collections/index.js'
import {
  deleteFromSearch,
  syncWithSearch,
  trackAfterChange,
  trackAfterDelete,
} from './hooks/index.js'

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
  (pluginConfig: PluginClickHouseConfig | true = {}): Plugin =>
  (incomingConfig: Config): Config => {
    // Handle true as shorthand for empty config
    const config = pluginConfig === true ? {} : pluginConfig
    const adminGroup = config.adminGroup ?? DEFAULT_ADMIN_GROUP
    const collections = [...(incomingConfig.collections || [])]

    // Add search collection
    if (config.search !== false) {
      const searchConfig = typeof config.search === 'object' ? config.search : undefined
      collections.push(generateSearchCollection(searchConfig, adminGroup))
    }

    // Add events collection
    if (config.events !== false) {
      const eventsConfig = typeof config.events === 'object' ? config.events : undefined
      collections.push(generateEventsCollection(eventsConfig, adminGroup))
    }

    // Add relationships collection
    if (config.relationships !== false) {
      const relationshipsConfig =
        typeof config.relationships === 'object' ? config.relationships : undefined
      collections.push(generateRelationshipsCollection(relationshipsConfig, adminGroup))
    }

    // Add actions collection
    if (config.actions !== false) {
      const actionsConfig = typeof config.actions === 'object' ? config.actions : undefined
      collections.push(generateActionsCollection(actionsConfig, adminGroup))
    }

    // Add data collection (disabled by default)
    if (config.data) {
      const dataConfig = typeof config.data === 'object' ? config.data : undefined
      collections.push(generateDataCollection(dataConfig, adminGroup))
    }

    // Inject search hooks into tracked collections
    if (config.search !== false && typeof config.search === 'object' && config.search.collections) {
      const searchConfig = config.search
      const trackedCollections = Object.keys(searchConfig.collections)

      for (let i = 0; i < collections.length; i++) {
        const collection = collections[i]
        if (collection && trackedCollections.includes(collection.slug)) {
          const collectionSearchConfig = searchConfig.collections[collection.slug]
          if (collectionSearchConfig) {
            collections[i] = {
              ...collection,
              hooks: {
                ...collection.hooks,
                afterChange: [
                  ...(collection.hooks?.afterChange || []),
                  syncWithSearch({
                    chunkOverlap: searchConfig.chunkOverlap,
                    chunkSize: searchConfig.chunkSize,
                    collectionSlug: collection.slug,
                    searchConfig: collectionSearchConfig,
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
    }

    // Inject event tracking hooks into collections
    if (config.events !== false) {
      const eventsConfig = typeof config.events === 'object' ? config.events : undefined

      if (eventsConfig?.trackCRUD !== false) {
        for (let i = 0; i < collections.length; i++) {
          const collection = collections[i]
          if (!collection) {continue}

          // Skip plugin-generated collections
          const pluginSlugs = ['search', 'events', 'relationships', 'actions', 'data']
          if (pluginSlugs.includes(collection.slug)) {continue}

          // Check collection-specific config
          const collectionConfig = eventsConfig?.collections?.[collection.slug]
          if (collectionConfig?.track === false) {continue}

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

    return {
      ...incomingConfig,
      collections,
    }
  }
