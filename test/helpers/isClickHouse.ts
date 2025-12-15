import type { Payload } from 'payload'

export const clickhouseList = ['clickhouse', 'do-clickhouse']

export function isClickHouse(_payload?: Payload) {
  return (
    _payload?.db?.name === 'clickhouse' ||
    clickhouseList.includes(process.env.PAYLOAD_DATABASE || '')
  )
}
