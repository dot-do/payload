import type { Init } from 'payload'

import type { ClickHouseAdapter } from './types.js'

/**
 * Initialize the ClickHouse adapter
 * For ClickHouse, we don't need to set up models since it's schemaless
 */
export const init: Init = async function init(this: ClickHouseAdapter): Promise<void> {
  // No initialization needed for ClickHouse
  // The table is created in connect()
}
