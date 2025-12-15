import type { BeginTransaction } from 'payload'

import type { BeginTransactionArgs, ClickHouseAdapter } from '../types.js'

import { generateId } from '../utilities/generateId.js'

export const beginTransaction: BeginTransaction = async function beginTransaction(
  this: ClickHouseAdapter,
  args?: BeginTransactionArgs,
): Promise<null | string> {
  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const timeout = args?.timeout !== undefined ? args.timeout : this.defaultTransactionTimeout
  const txId = generateId(this.idType)
  const now = Date.now()

  // Insert transaction metadata row
  const query = `
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
      'pending',
      ${timeout !== null ? 'fromUnixTimestamp64Milli({txTimeout:Int64})' : 'NULL'},
      fromUnixTimestamp64Milli({txCreatedAt:Int64}),
      '_tx_metadata',
      {ns:String},
      '_tx_metadata',
      fromUnixTimestamp64Milli({v:Int64}),
      '{}',
      '',
      fromUnixTimestamp64Milli({createdAt:Int64}),
      fromUnixTimestamp64Milli({updatedAt:Int64})
    )
  `

  const params: Record<string, unknown> = {
    createdAt: now,
    ns: this.namespace,
    txCreatedAt: now,
    txId,
    updatedAt: now,
    v: now,
  }

  if (timeout !== null) {
    params.txTimeout = now + timeout
  }

  await this.clickhouse.command({
    query,
    query_params: params,
  })

  return txId
}
