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
    access: {
      create: () => false, // Created via API only
      delete: config?.access?.delete ?? (({ req }) => !!req.user),
      read: config?.access?.read ?? (({ req }) => !!req.user),
      update: config?.access?.update ?? (({ req }) => !!req.user),
    },
    admin: {
      defaultColumns: ['name', 'type', 'status', 'collection', 'createdAt'],
      description:
        'Task and job orchestration queue from ClickHouse. Monitor background jobs, workflows, and human-in-the-loop tasks.',
      group: adminGroup,
      useAsTitle: 'name',
      ...(config?.overrides?.admin || {}),
    },
    fields: [
      // Identity
      {
        name: 'type',
        type: 'select',
        admin: { readOnly: true },
        options: [
          { label: 'Transaction', value: 'transaction' },
          { label: 'Job', value: 'job' },
          { label: 'Workflow', value: 'workflow' },
          { label: 'Task', value: 'task' },
        ],
        required: true,
      },
      {
        name: 'name',
        type: 'text',
        admin: { readOnly: true },
        required: true,
      },
      // Status
      {
        name: 'status',
        type: 'select',
        admin: { position: 'sidebar' },
        defaultValue: 'pending',
        options: [
          { label: 'Pending', value: 'pending' },
          { label: 'Running', value: 'running' },
          { label: 'Waiting', value: 'waiting' },
          { label: 'Completed', value: 'completed' },
          { label: 'Failed', value: 'failed' },
          { label: 'Cancelled', value: 'cancelled' },
        ],
        required: true,
      },
      {
        name: 'priority',
        type: 'number',
        admin: { position: 'sidebar' },
        defaultValue: 0,
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
          condition: (data) => data?.status === 'failed',
          readOnly: true,
        },
      },
      // Workflow state
      {
        name: 'step',
        type: 'number',
        admin: { position: 'sidebar', readOnly: true },
        defaultValue: 0,
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
        admin: {
          condition: (data) => data?.status === 'waiting',
          position: 'sidebar',
        },
        options: [
          { label: 'Input', value: 'input' },
          { label: 'Approval', value: 'approval' },
          { label: 'Review', value: 'review' },
          { label: 'External', value: 'external' },
        ],
      },
      // Timing
      {
        name: 'scheduledAt',
        type: 'date',
        admin: { position: 'sidebar', readOnly: true },
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
        admin: { position: 'sidebar', readOnly: true },
      },
      // Retry
      {
        name: 'attempts',
        type: 'number',
        admin: { position: 'sidebar', readOnly: true },
        defaultValue: 0,
      },
      {
        name: 'maxAttempts',
        type: 'number',
        admin: { position: 'sidebar', readOnly: true },
        defaultValue: 3,
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
        admin: { position: 'sidebar', readOnly: true },
      },
      {
        name: 'rootId',
        type: 'text',
        admin: { position: 'sidebar', readOnly: true },
      },
    ],
    labels: {
      plural: 'Actions',
      singular: 'Action',
    },
    ...(config?.overrides || {}),
  }
}
