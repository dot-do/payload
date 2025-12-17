/**
 * Example Cloudflare Worker for db-rpc-server
 *
 * This is a minimal example showing how to expose a database adapter
 * via RPC in a Cloudflare Worker.
 */

/// <reference types="@cloudflare/workers-types" />

// In a real implementation, you would import:
// import { createRpcServer } from '@dotdo/db-rpc-server/hono'
// import { getPayload } from 'payload'

export interface Env {
  // Add your bindings here (e.g., D1 database, KV, etc.)
  DB: D1Database
}

/**
 * In a real implementation, you would:
 * 1. Initialize Payload with a D1 or other Workers-compatible adapter
 * 2. Pass the adapter and Payload instance to createRpcServer
 *
 * This example shows the structure but requires actual adapter setup.
 */
const handler: ExportedHandler<Env> = {
  // eslint-disable-next-line @typescript-eslint/require-await
  async fetch(_request, _env, _ctx) {
    // TODO: Initialize Payload with D1 adapter
    // const payload = await getPayload({ config })

    // For now, return a placeholder response
    // In production, you would create the RPC server like this:
    //
    // const app = createRpcServer({
    //   adapter: payload.db,
    //   payload,
    // })
    // return app.fetch(request, env, ctx)

    return new Response(
      JSON.stringify({
        message: 'db-rpc-server worker - configure with your Payload instance',
        note: 'See README for setup instructions',
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      },
    )
  },
}

// eslint-disable-next-line no-restricted-exports
export default handler
