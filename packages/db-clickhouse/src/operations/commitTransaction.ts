import type { CommitTransaction } from 'payload'

import type { ClickHouseAdapter } from '../types.js'

export const commitTransaction: CommitTransaction = async function commitTransaction(
  this: ClickHouseAdapter,
  txId: null | string,
): Promise<void> {
  if (!txId) {
    return
  }

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  // Copy all pending actions for this transaction to the data table
  const copyQuery = `
    INSERT INTO ${this.table} (
      ns,
      type,
      id,
      v,
      title,
      data,
      createdAt,
      createdBy,
      updatedAt,
      updatedBy,
      deletedAt,
      deletedBy
    )
    SELECT
      ns,
      type,
      id,
      v,
      title,
      data,
      createdAt,
      createdBy,
      updatedAt,
      updatedBy,
      deletedAt,
      deletedBy
    FROM actions
    WHERE txId = {txId:String}
      AND txStatus = 'pending'
      AND type != '_tx_metadata'
  `

  await this.clickhouse.command({
    query: copyQuery,
    query_params: { txId },
  })

  // Insert a row to mark the transaction as committed
  const now = Date.now()
  const commitQuery = `
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
      'committed',
      NULL,
      fromUnixTimestamp64Milli({txCreatedAt:Int64}),
      '_tx_committed',
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
    query: commitQuery,
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
