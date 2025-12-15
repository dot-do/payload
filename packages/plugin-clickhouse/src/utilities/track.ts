// packages/plugin-clickhouse/src/utilities/track.ts
import type { Payload } from 'payload'

// Type for ClickHouse adapter methods (not all adapters have these)
interface ClickHouseDb {
  logEvent: (args: {
    collection?: string
    docId?: string
    duration?: number
    input?: Record<string, unknown>
    ip?: string
    result?: Record<string, unknown>
    sessionId?: string
    type: string
    userId?: string
  }) => Promise<string>
}

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
  return async (type: string, args: TrackArgs = {}): Promise<string | undefined> => {
    const db = payload.db as unknown as ClickHouseDb

    if (typeof db.logEvent !== 'function') {
      payload.logger.warn('logEvent not available on database adapter - track() is a no-op')
      return undefined
    }

    return await db.logEvent({
      type,
      collection: args.collection,
      docId: args.docId,
      duration: args.duration,
      input: args.input,
      ip: args.ip,
      result: args.result,
      sessionId: args.sessionId,
      userId: args.userId,
    })
  }
}
