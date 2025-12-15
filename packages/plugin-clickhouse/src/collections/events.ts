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
    access: {
      // Immutable - no create, update, or delete
      create: () => false,
      delete: () => false,
      read: config?.access?.read ?? (({ req }) => !!req.user),
      update: () => false,
    },
    admin: {
      defaultColumns: ['type', 'collection', 'docId', 'userId', 'timestamp'],
      description: 'Immutable audit log of system and custom events from ClickHouse.',
      group: adminGroup,
      useAsTitle: 'type',
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
        admin: { position: 'sidebar', readOnly: true },
      },
      {
        name: 'sessionId',
        type: 'text',
        admin: { position: 'sidebar', readOnly: true },
      },
      {
        name: 'ip',
        type: 'text',
        admin: { position: 'sidebar', readOnly: true },
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
        admin: { position: 'sidebar', readOnly: true },
      },
      {
        name: 'timestamp',
        type: 'date',
        admin: {
          date: { displayFormat: 'PPpp' },
          readOnly: true,
        },
      },
    ],
    labels: {
      plural: 'Events',
      singular: 'Event',
    },
    ...(config?.overrides || {}),
  }
}
