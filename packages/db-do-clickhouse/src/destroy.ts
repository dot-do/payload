import type { Destroy } from 'payload'

import type { DOClickHouseAdapter } from './types.js'

import { closeClickHouseClient } from './clickhouse/client.js'

export const destroy: Destroy = async function destroy(this: DOClickHouseAdapter): Promise<void> {
  // Close ClickHouse connection
  await closeClickHouseClient(this.clickhouse)
  this.clickhouse = null
}
