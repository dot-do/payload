import type { ClickHouseAdapter, LogEventArgs } from '../types.js'

import { generateUlid } from '../utilities/generateId.js'

export async function logEvent(this: ClickHouseAdapter, args: LogEventArgs): Promise<string> {
  const {
    type,
    collection,
    docId,
    duration = 0,
    input = {},
    ip,
    result = {},
    sessionId,
    userId,
  } = args

  if (!this.clickhouse) {
    throw new Error('ClickHouse client not connected')
  }

  const id = generateUlid()
  const now = Date.now()

  const query = `
    INSERT INTO events (id, ns, timestamp, type, collection, docId, userId, sessionId, ip, duration, input, result)
    VALUES (
      {id:String},
      {ns:String},
      fromUnixTimestamp64Milli({timestamp:Int64}),
      {type:String},
      ${collection ? '{collection:String}' : 'NULL'},
      ${docId ? '{docId:String}' : 'NULL'},
      ${userId ? '{userId:String}' : 'NULL'},
      ${sessionId ? '{sessionId:String}' : 'NULL'},
      ${ip ? '{ip:String}' : 'NULL'},
      {duration:UInt32},
      {input:String},
      {result:String}
    )
  `

  const params: Record<string, unknown> = {
    id,
    type,
    duration,
    input: JSON.stringify(input),
    ns: this.namespace,
    result: JSON.stringify(result),
    timestamp: now,
  }

  if (collection) {
    params.collection = collection
  }
  if (docId) {
    params.docId = docId
  }
  if (userId) {
    params.userId = userId
  }
  if (sessionId) {
    params.sessionId = sessionId
  }
  if (ip) {
    params.ip = ip
  }

  await this.clickhouse.command({
    query,
    query_params: params,
  })

  return id
}
