// packages/plugin-clickhouse/src/hooks/trackEvent.ts
import type { CollectionAfterChangeHook, CollectionAfterDeleteHook } from 'payload'

import type { EventsCollectionConfig } from '../types.js'

interface TrackEventArgs {
  collectionSlug: string
  eventConfig?: EventsCollectionConfig
}

// Type for ClickHouse adapter methods (not all adapters have these)
interface ClickHouseDb {
  logEvent: (args: {
    collection?: string
    docId?: string
    input?: Record<string, unknown>
    ip?: string
    result?: Record<string, unknown>
    sessionId?: string
    type: string
    userId?: string
  }) => Promise<unknown>
}

export const trackAfterChange =
  ({ collectionSlug, eventConfig }: TrackEventArgs): CollectionAfterChangeHook =>
  async ({ doc, operation, previousDoc, req }) => {
    const { payload } = req
    const db = payload.db as unknown as ClickHouseDb

    if (typeof db.logEvent !== 'function') {
      return doc
    }

    const eventType = operation === 'create' ? 'doc.create' : 'doc.update'
    const includeInput = eventConfig?.includeInput !== false

    try {
      await db.logEvent({
        type: eventType,
        collection: collectionSlug,
        docId: String(doc.id),
        input: includeInput ? { hasChanges: !!previousDoc, operation } : undefined,
        ip: req.headers?.get?.('x-forwarded-for') || req.headers?.get?.('x-real-ip') || undefined,
        result: { success: true },
        sessionId: req.headers?.get?.('x-session-id') || undefined,
        userId: req.user?.id ? String(req.user.id) : undefined,
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
  ({ collectionSlug, eventConfig }: TrackEventArgs): CollectionAfterDeleteHook =>
  async ({ id, doc, req }) => {
    const { payload } = req
    const db = payload.db as unknown as ClickHouseDb

    if (typeof db.logEvent !== 'function') {
      return doc
    }

    try {
      await db.logEvent({
        type: 'doc.delete',
        collection: collectionSlug,
        docId: String(id),
        ip: req.headers?.get?.('x-forwarded-for') || req.headers?.get?.('x-real-ip') || undefined,
        result: { success: true },
        sessionId: req.headers?.get?.('x-session-id') || undefined,
        userId: req.user?.id ? String(req.user.id) : undefined,
      })
    } catch (error) {
      payload.logger.error({
        err: error,
        msg: `Failed to log doc.delete event for ${collectionSlug}/${id}`,
      })
    }

    return doc
  }
