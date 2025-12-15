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
    access: {
      create: () => false,
      delete: () => false,
      read: config?.access?.read ?? (() => true),
      update: () => false,
    },
    admin: {
      defaultColumns: ['collection', 'docId', 'status', 'updatedAt'],
      description: 'Full-text and vector search index entries synced from ClickHouse.',
      group: adminGroup,
      useAsTitle: 'text',
      ...(config?.overrides?.admin || {}),
    },
    fields: [
      {
        name: 'collection',
        type: 'text',
        admin: { readOnly: true },
        required: true,
      },
      {
        name: 'docId',
        type: 'text',
        admin: { readOnly: true },
        required: true,
      },
      {
        name: 'text',
        type: 'textarea',
        admin: { readOnly: true },
      },
      {
        name: 'chunkIndex',
        type: 'number',
        admin: { position: 'sidebar', readOnly: true },
        defaultValue: 0,
      },
      {
        name: 'status',
        type: 'select',
        admin: { position: 'sidebar', readOnly: true },
        defaultValue: 'pending',
        options: [
          { label: 'Pending', value: 'pending' },
          { label: 'Ready', value: 'ready' },
          { label: 'Failed', value: 'failed' },
        ],
      },
      {
        name: 'error',
        type: 'text',
        admin: {
          condition: (data) => data?.status === 'failed',
          readOnly: true,
        },
      },
    ],
    labels: {
      plural: 'Search Index',
      singular: 'Search Index',
    },
    ...(config?.overrides || {}),
  }
}
