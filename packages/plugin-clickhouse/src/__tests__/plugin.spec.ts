import type { CollectionConfig, Config } from 'payload'

import { defaults } from 'payload'

import { clickhousePlugin } from '../index.js'

describe('@dotdo/plugin-clickhouse - unit', () => {
  describe('plugin initialization', () => {
    it('should run with default config', () => {
      const plugin = clickhousePlugin()
      const config = plugin(createConfig())

      // All collections should be added by default except data
      expect(findCollection(config, 'search')).toBeDefined()
      expect(findCollection(config, 'events')).toBeDefined()
      expect(findCollection(config, 'relationships')).toBeDefined()
      expect(findCollection(config, 'actions')).toBeDefined()
      expect(findCollection(config, 'data')).toBeUndefined()
    })

    it('should run with true as shorthand', () => {
      const plugin = clickhousePlugin(true)
      const config = plugin(createConfig())

      expect(findCollection(config, 'search')).toBeDefined()
      expect(findCollection(config, 'events')).toBeDefined()
    })

    it('should apply custom adminGroup to all collections', () => {
      const plugin = clickhousePlugin({ adminGroup: 'Analytics' })
      const config = plugin(createConfig())

      const searchCollection = findCollection(config, 'search')
      const eventsCollection = findCollection(config, 'events')
      const relationshipsCollection = findCollection(config, 'relationships')
      const actionsCollection = findCollection(config, 'actions')

      expect(searchCollection?.admin?.group).toBe('Analytics')
      expect(eventsCollection?.admin?.group).toBe('Analytics')
      expect(relationshipsCollection?.admin?.group).toBe('Analytics')
      expect(actionsCollection?.admin?.group).toBe('Analytics')
    })
  })

  describe('search collection', () => {
    it('should be disabled when search: false', () => {
      const plugin = clickhousePlugin({ search: false })
      const config = plugin(createConfig())

      expect(findCollection(config, 'search')).toBeUndefined()
    })

    it('should use custom slug', () => {
      const plugin = clickhousePlugin({
        search: { slug: 'search-index' },
      })
      const config = plugin(createConfig())

      expect(findCollection(config, 'search')).toBeUndefined()
      expect(findCollection(config, 'search-index')).toBeDefined()
    })

    it('should override adminGroup per-collection', () => {
      const plugin = clickhousePlugin({
        adminGroup: 'Analytics',
        search: { adminGroup: 'Search' },
      })
      const config = plugin(createConfig())

      const searchCollection = findCollection(config, 'search')
      expect(searchCollection?.admin?.group).toBe('Search')
    })

    it('should have required fields', () => {
      const plugin = clickhousePlugin()
      const config = plugin(createConfig())

      const searchCollection = findCollection(config, 'search')
      expect(searchCollection).toBeDefined()

      const fieldNames = searchCollection?.fields?.map((f) => ('name' in f ? f.name : undefined))
      expect(fieldNames).toContain('collection')
      expect(fieldNames).toContain('docId')
      expect(fieldNames).toContain('text')
      expect(fieldNames).toContain('status')
    })

    it('should be read-only', () => {
      const plugin = clickhousePlugin()
      const config = plugin(createConfig())

      const searchCollection = findCollection(config, 'search')

      // Test access function returns false for create/update/delete
      const mockReq = {} as any
      expect(searchCollection?.access?.create?.({ req: mockReq })).toBe(false)
      expect(searchCollection?.access?.update?.({ req: mockReq })).toBe(false)
      expect(searchCollection?.access?.delete?.({ req: mockReq })).toBe(false)
    })
  })

  describe('events collection', () => {
    it('should be disabled when events: false', () => {
      const plugin = clickhousePlugin({ events: false })
      const config = plugin(createConfig())

      expect(findCollection(config, 'events')).toBeUndefined()
    })

    it('should use custom slug', () => {
      const plugin = clickhousePlugin({
        events: { slug: 'audit-log' },
      })
      const config = plugin(createConfig())

      expect(findCollection(config, 'events')).toBeUndefined()
      expect(findCollection(config, 'audit-log')).toBeDefined()
    })

    it('should be completely immutable', () => {
      const plugin = clickhousePlugin()
      const config = plugin(createConfig())

      const eventsCollection = findCollection(config, 'events')

      // Test access function returns false for all write operations
      const mockReq = {} as any
      expect(eventsCollection?.access?.create?.({ req: mockReq })).toBe(false)
      expect(eventsCollection?.access?.update?.({ req: mockReq })).toBe(false)
      expect(eventsCollection?.access?.delete?.({ req: mockReq })).toBe(false)
    })

    it('should have required fields', () => {
      const plugin = clickhousePlugin()
      const config = plugin(createConfig())

      const eventsCollection = findCollection(config, 'events')
      const fieldNames = eventsCollection?.fields?.map((f) => ('name' in f ? f.name : undefined))

      expect(fieldNames).toContain('type')
      expect(fieldNames).toContain('collection')
      expect(fieldNames).toContain('docId')
      expect(fieldNames).toContain('userId')
      expect(fieldNames).toContain('timestamp')
    })
  })

  describe('relationships collection', () => {
    it('should be disabled when relationships: false', () => {
      const plugin = clickhousePlugin({ relationships: false })
      const config = plugin(createConfig())

      expect(findCollection(config, 'relationships')).toBeUndefined()
    })

    it('should use custom slug', () => {
      const plugin = clickhousePlugin({
        relationships: { slug: 'document-links' },
      })
      const config = plugin(createConfig())

      expect(findCollection(config, 'relationships')).toBeUndefined()
      expect(findCollection(config, 'document-links')).toBeDefined()
    })

    it('should be read-only', () => {
      const plugin = clickhousePlugin()
      const config = plugin(createConfig())

      const relationshipsCollection = findCollection(config, 'relationships')

      const mockReq = {} as any
      expect(relationshipsCollection?.access?.create?.({ req: mockReq })).toBe(false)
      expect(relationshipsCollection?.access?.update?.({ req: mockReq })).toBe(false)
      expect(relationshipsCollection?.access?.delete?.({ req: mockReq })).toBe(false)
    })

    it('should have required fields', () => {
      const plugin = clickhousePlugin()
      const config = plugin(createConfig())

      const relationshipsCollection = findCollection(config, 'relationships')
      const fieldNames = relationshipsCollection?.fields?.map((f) =>
        'name' in f ? f.name : undefined,
      )

      expect(fieldNames).toContain('fromType')
      expect(fieldNames).toContain('fromId')
      expect(fieldNames).toContain('toType')
      expect(fieldNames).toContain('toId')
      expect(fieldNames).toContain('fromField')
    })
  })

  describe('actions collection', () => {
    it('should be disabled when actions: false', () => {
      const plugin = clickhousePlugin({ actions: false })
      const config = plugin(createConfig())

      expect(findCollection(config, 'actions')).toBeUndefined()
    })

    it('should use custom slug', () => {
      const plugin = clickhousePlugin({
        actions: { slug: 'tasks' },
      })
      const config = plugin(createConfig())

      expect(findCollection(config, 'actions')).toBeUndefined()
      expect(findCollection(config, 'tasks')).toBeDefined()
    })

    it('should have required fields', () => {
      const plugin = clickhousePlugin()
      const config = plugin(createConfig())

      const actionsCollection = findCollection(config, 'actions')
      const fieldNames = actionsCollection?.fields?.map((f) => ('name' in f ? f.name : undefined))

      expect(fieldNames).toContain('type')
      expect(fieldNames).toContain('name')
      expect(fieldNames).toContain('status')
      expect(fieldNames).toContain('priority')
      expect(fieldNames).toContain('input')
      expect(fieldNames).toContain('output')
    })
  })

  describe('data collection', () => {
    it('should be disabled by default', () => {
      const plugin = clickhousePlugin()
      const config = plugin(createConfig())

      expect(findCollection(config, 'data')).toBeUndefined()
    })

    it('should be enabled when data: true', () => {
      const plugin = clickhousePlugin({ data: true })
      const config = plugin(createConfig())

      expect(findCollection(config, 'data')).toBeDefined()
    })

    it('should use custom slug', () => {
      const plugin = clickhousePlugin({
        data: { slug: 'raw-data' },
      })
      const config = plugin(createConfig())

      expect(findCollection(config, 'data')).toBeUndefined()
      expect(findCollection(config, 'raw-data')).toBeDefined()
    })
  })

  describe('search hooks injection', () => {
    it('should inject hooks into tracked collections', () => {
      const postsCollection: CollectionConfig = {
        slug: 'posts',
        fields: [
          { name: 'title', type: 'text' },
          { name: 'content', type: 'textarea' },
        ],
      }

      const plugin = clickhousePlugin({
        events: false, // Disable events to isolate search hook testing
        search: {
          collections: {
            posts: { fields: ['title', 'content'] },
          },
        },
      })

      const config = plugin(
        createConfig({
          collections: [postsCollection],
        }),
      )

      const posts = findCollection(config, 'posts')
      expect(posts?.hooks?.afterChange).toHaveLength(1)
      expect(posts?.hooks?.beforeDelete).toHaveLength(1)
    })

    it('should not inject hooks when search is disabled', () => {
      const postsCollection: CollectionConfig = {
        slug: 'posts',
        fields: [{ name: 'title', type: 'text' }],
      }

      const plugin = clickhousePlugin({ search: false, events: false })
      const config = plugin(
        createConfig({
          collections: [postsCollection],
        }),
      )

      const posts = findCollection(config, 'posts')
      expect(posts?.hooks?.afterChange).toBeUndefined()
    })
  })

  describe('event tracking hooks injection', () => {
    it('should inject hooks into user collections when trackCRUD is enabled', () => {
      const postsCollection: CollectionConfig = {
        slug: 'posts',
        fields: [{ name: 'title', type: 'text' }],
      }

      const plugin = clickhousePlugin({
        events: { trackCRUD: true },
      })

      const config = plugin(
        createConfig({
          collections: [postsCollection],
        }),
      )

      const posts = findCollection(config, 'posts')
      expect(posts?.hooks?.afterChange?.length).toBeGreaterThan(0)
      expect(posts?.hooks?.afterDelete?.length).toBeGreaterThan(0)
    })

    it('should skip plugin-generated collections', () => {
      const plugin = clickhousePlugin({
        events: { trackCRUD: true },
      })
      const config = plugin(createConfig())

      // Plugin collections should not have tracking hooks
      const eventsCollection = findCollection(config, 'events')
      const searchCollection = findCollection(config, 'search')

      // These collections should only have their default hooks, not tracking hooks
      expect(eventsCollection?.hooks?.afterChange).toBeUndefined()
      expect(searchCollection?.hooks?.afterChange).toBeUndefined()
    })

    it('should respect collection-specific track: false', () => {
      const postsCollection: CollectionConfig = {
        slug: 'posts',
        fields: [{ name: 'title', type: 'text' }],
      }

      const sessionsCollection: CollectionConfig = {
        slug: 'sessions',
        fields: [{ name: 'token', type: 'text' }],
      }

      const plugin = clickhousePlugin({
        events: {
          trackCRUD: true,
          collections: {
            sessions: { track: false },
          },
        },
      })

      const config = plugin(
        createConfig({
          collections: [postsCollection, sessionsCollection],
        }),
      )

      const posts = findCollection(config, 'posts')
      const sessions = findCollection(config, 'sessions')

      expect(posts?.hooks?.afterChange?.length).toBeGreaterThan(0)
      expect(sessions?.hooks?.afterChange).toBeUndefined()
    })
  })

  describe('combined configuration', () => {
    it('should support full configuration', () => {
      const plugin = clickhousePlugin({
        adminGroup: 'Analytics',
        search: {
          slug: 'search-index',
          collections: {
            posts: { fields: ['title', 'content'] },
          },
          chunkSize: 500,
        },
        events: {
          slug: 'audit-log',
          trackCRUD: true,
          collections: {
            users: { includeInput: false },
          },
        },
        relationships: {
          slug: 'document-links',
          adminGroup: 'Debug',
        },
        actions: {
          slug: 'tasks',
        },
        data: {
          slug: 'raw-data',
          adminGroup: 'Debug',
        },
      })

      const config = plugin(createConfig())

      expect(findCollection(config, 'search-index')).toBeDefined()
      expect(findCollection(config, 'audit-log')).toBeDefined()
      expect(findCollection(config, 'document-links')).toBeDefined()
      expect(findCollection(config, 'tasks')).toBeDefined()
      expect(findCollection(config, 'raw-data')).toBeDefined()

      // Check specific adminGroup overrides
      const relationships = findCollection(config, 'document-links')
      const data = findCollection(config, 'raw-data')
      expect(relationships?.admin?.group).toBe('Debug')
      expect(data?.admin?.group).toBe('Debug')
    })
  })
})

function findCollection(config: Config, slug: string): CollectionConfig | undefined {
  return config.collections?.find((c) => c.slug === slug)
}

function createConfig(overrides?: Partial<Config>): Config {
  return {
    ...defaults,
    ...overrides,
  } as Config
}
