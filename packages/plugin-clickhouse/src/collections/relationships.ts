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
    access: {
      // Read-only - managed by db-adapter
      create: () => false,
      delete: () => false,
      read: config?.access?.read ?? (({ req }) => !!req.user),
      update: () => false,
    },
    admin: {
      defaultColumns: ['fromType', 'fromId', 'fromField', 'toType', 'toId'],
      description:
        'Document relationship graph from ClickHouse. Shows how documents link to each other.',
      group: adminGroup,
      ...(config?.overrides?.admin || {}),
    },
    fields: [
      {
        name: 'fromType',
        type: 'text',
        admin: { readOnly: true },
        required: true,
      },
      {
        name: 'fromId',
        type: 'text',
        admin: { readOnly: true },
        required: true,
      },
      {
        name: 'fromField',
        type: 'text',
        admin: { readOnly: true },
        required: true,
      },
      {
        name: 'toType',
        type: 'text',
        admin: { readOnly: true },
        required: true,
      },
      {
        name: 'toId',
        type: 'text',
        admin: { readOnly: true },
        required: true,
      },
      {
        name: 'position',
        type: 'number',
        admin: { position: 'sidebar', readOnly: true },
      },
      {
        name: 'locale',
        type: 'text',
        admin: { position: 'sidebar', readOnly: true },
      },
    ],
    labels: {
      plural: 'Relationships',
      singular: 'Relationship',
    },
    ...(config?.overrides || {}),
  }
}
