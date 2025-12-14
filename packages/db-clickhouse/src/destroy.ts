import type { Destroy } from 'payload'

import type { ClickHouseAdapter } from './types.js'

/**
 * Close the ClickHouse connection
 */
export const destroy: Destroy = async function destroy(this: ClickHouseAdapter): Promise<void> {
  if (this.client) {
    await this.client.close()
    this.client = null
  }
}
