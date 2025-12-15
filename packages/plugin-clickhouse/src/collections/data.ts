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
    access: {
      // Read-only debug view
      create: () => false,
      delete: () => false,
      read: config?.access?.read ?? (({ req }) => req.user?.role === 'admin'),
      update: () => false,
    },
    admin: {
      defaultColumns: ['type', 'title', 'id', 'updatedAt'],
      description: 'Raw document data from ClickHouse. Debug view of the underlying data table.',
      group: adminGroup,
      useAsTitle: 'title',
      ...(config?.overrides?.admin || {}),
    },
    fields: [
      {
        name: 'type',
        type: 'text',
        admin: { readOnly: true },
        required: true,
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
        admin: { position: 'sidebar', readOnly: true },
      },
      {
        name: 'updatedBy',
        type: 'text',
        admin: { position: 'sidebar', readOnly: true },
      },
      {
        name: 'deletedAt',
        type: 'date',
        admin: { position: 'sidebar', readOnly: true },
      },
      {
        name: 'deletedBy',
        type: 'text',
        admin: { position: 'sidebar', readOnly: true },
      },
    ],
    labels: {
      plural: 'Data Records',
      singular: 'Data Record',
    },
    ...(config?.overrides || {}),
  }
}
