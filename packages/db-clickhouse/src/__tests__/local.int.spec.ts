import fs from 'fs'
import os from 'os'
import path from 'path'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { createChdbClient } from '../local/chdbClient.js'

import type { ChdbClient } from '../local/chdbClient.js'

describe.skipIf(!process.env.CHDB_TEST)('ChdbClient Integration Tests', () => {
  let client: ChdbClient
  let testDir: string

  beforeAll(() => {
    // Create a temporary directory for the test
    testDir = path.join(os.tmpdir(), `chdb-test-${Date.now()}`)
    fs.mkdirSync(testDir, { recursive: true })
    client = createChdbClient(testDir)
  })

  afterAll(async () => {
    // Cleanup
    await client.close()
    // Remove test directory
    fs.rmSync(testDir, { recursive: true, force: true })
  })

  it('should create a table', async () => {
    await client.command({
      query: `
        CREATE TABLE IF NOT EXISTS test_data (
          id String,
          name String,
          value Int32
        ) ENGINE = MergeTree()
        ORDER BY id
      `,
    })

    // Verify table exists
    const result = await client.query({
      format: 'JSONEachRow',
      query: 'SHOW TABLES',
    })
    const tables = await result.json<{ name: string }>()
    expect(tables.some((t) => t.name === 'test_data')).toBe(true)
  })

  it('should insert and query data', async () => {
    await client.command({
      query: `
        INSERT INTO test_data (id, name, value) VALUES
        ('1', 'Test One', 100),
        ('2', 'Test Two', 200)
      `,
    })

    const result = await client.query({
      format: 'JSONEachRow',
      query: 'SELECT * FROM test_data ORDER BY id',
    })
    const rows = await result.json<{ id: string; name: string; value: number }>()

    expect(rows).toHaveLength(2)
    expect(rows[0]).toEqual({ id: '1', name: 'Test One', value: 100 })
    expect(rows[1]).toEqual({ id: '2', name: 'Test Two', value: 200 })
  })

  it('should support parameterized queries', async () => {
    const result = await client.query({
      format: 'JSONEachRow',
      query: 'SELECT * FROM test_data WHERE id = {id:String}',
      query_params: { id: '1' },
    })
    const rows = await result.json<{ id: string; name: string; value: number }>()

    expect(rows).toHaveLength(1)
    expect(rows[0]).toEqual({ id: '1', name: 'Test One', value: 100 })
  })

  it('should handle count queries', async () => {
    const result = await client.query({
      format: 'JSONEachRow',
      query: 'SELECT count() as total FROM test_data',
    })
    const rows = await result.json<{ total: string }>()

    expect(rows).toHaveLength(1)
    expect(parseInt(rows[0]!.total, 10)).toBe(2)
  })
})
