import type { Payload } from 'payload'

export function isClickHouse(payload: Payload): boolean {
  return payload.db.name === 'clickhouse'
}
