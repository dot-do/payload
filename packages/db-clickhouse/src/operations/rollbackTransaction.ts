import type { RollbackTransaction } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

export const rollbackTransaction: RollbackTransaction = async function rollbackTransaction(
  this: ClickHouseAdapter,
  txId: null | string,
): Promise<void> {
  if (!txId) {
    return
  }

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  // Insert a row to mark the transaction as aborted
  const now = Date.now()
  const abortQuery = `
    INSERT INTO actions (
      txId,
      txStatus,
      txTimeout,
      txCreatedAt,
      id,
      ns,
      type,
      v,
      data,
      title,
      createdAt,
      updatedAt
    )
    VALUES (
      {txId:String},
      'aborted',
      NULL,
      fromUnixTimestamp64Milli({txCreatedAt:Int64}),
      '_tx_aborted',
      {ns:String},
      '_tx_metadata',
      fromUnixTimestamp64Milli({v:Int64}),
      '{}',
      '',
      fromUnixTimestamp64Milli({createdAt:Int64}),
      fromUnixTimestamp64Milli({updatedAt:Int64})
    )
  `

  await this.clickhouse.command({
    query: abortQuery,
    query_params: {
      createdAt: now,
      ns: this.namespace,
      txCreatedAt: now,
      txId,
      updatedAt: now,
      v: now,
    },
  })
}
