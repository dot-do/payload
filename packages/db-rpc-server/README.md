# @dotdo/db-rpc-server

RPC server for Payload database adapters. Exposes any Payload database adapter via HTTP/WebSocket using [capnweb](https://github.com/cloudflare/capnweb) and [Hono](https://hono.dev).

## Installation

```bash
pnpm add @dotdo/db-rpc-server hono
```

## Quick Start

### Cloudflare Workers

```typescript
import { createRpcServer } from '@dotdo/db-rpc-server/hono'
import { getPayload } from 'payload'
import config from './payload.config'

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    const payload = await getPayload({ config })

    const app = createRpcServer({
      adapter: payload.db,
      payload,
    })

    return app.fetch(request, env, ctx)
  },
}
```

### Bun

```typescript
import { createRpcServer } from '@dotdo/db-rpc-server/hono'
import { getPayload } from 'payload'
import config from './payload.config'

const payload = await getPayload({ config })

const app = createRpcServer({
  adapter: payload.db,
  payload,
})

export default {
  fetch: app.fetch,
  port: 3001,
}
```

### Node.js with @hono/node-server

```typescript
import { serve } from '@hono/node-server'
import { createRpcServer } from '@dotdo/db-rpc-server/hono'
import { getPayload } from 'payload'
import config from './payload.config'

const payload = await getPayload({ config })

const app = createRpcServer({
  adapter: payload.db,
  payload,
})

serve({
  fetch: app.fetch,
  port: 3001,
})
```

### Adding to Existing Hono App

```typescript
import { Hono } from 'hono'
import { createRpcMiddleware } from '@dotdo/db-rpc-server/hono'
import { getPayload } from 'payload'
import config from './payload.config'

const payload = await getPayload({ config })

const app = new Hono()

// Your other routes
app.get('/', (c) => c.text('Hello'))

// Mount RPC at /rpc
app.route(
  '/rpc',
  createRpcMiddleware({
    adapter: payload.db,
    payload,
  }),
)

export default app
```

## Authentication

The server supports two authentication modes:

### Option 1: Payload Auth (Default)

Uses Payload's built-in authentication system. Pass the `payload` instance:

```typescript
createRpcServer({
  adapter: payload.db,
  payload, // Uses payload.auth() to validate tokens
})
```

**Supported token types:**

- JWT Tokens - Obtained from `payload.login()`
- API Keys - If configured in your users collection

**Getting a token:**

```typescript
const { token } = await payload.login({
  collection: 'users',
  data: {
    email: 'user@example.com',
    password: 'password',
  },
})
```

### Option 2: Custom Auth

Use `validateToken` for external auth systems (Cloudflare Access, custom JWT, etc.):

```typescript
createRpcServer({
  adapter: db,
  validateToken: async (token) => {
    // Validate with your auth system
    const decoded = await verifyJwt(token, { secret: env.JWT_SECRET })
    if (!decoded) return null

    // Return a user object (must have at least 'id')
    return {
      id: decoded.sub,
      email: decoded.email,
      // ... other user properties
    }
  },
})
```

**Cloudflare Access example:**

```typescript
createRpcServer({
  adapter: db,
  validateToken: async (token) => {
    // Validate Cloudflare Access JWT
    const payload = await verifyCloudflareAccessJwt(token, {
      teamDomain: env.CF_TEAM_DOMAIN,
      audience: env.CF_ACCESS_AUD,
    })

    return payload ? { id: payload.sub, email: payload.email } : null
  },
})
```

### Auth Flow

1. Client connects to the public RPC endpoint
2. Client calls `authenticate(bearerToken)`
3. Server validates token (via Payload or custom validator)
4. Server returns an authenticated API stub
5. All subsequent calls on that stub include user context

## API Reference

### `createRpcServer(options)`

Creates a standalone Hono application with RPC endpoint.

```typescript
interface RpcServerOptions {
  adapter: BaseDatabaseAdapter
  payload?: Payload // For Payload auth
  validateToken?: ValidateTokenFn // For custom auth
  basePath?: string // default: '/rpc'
}

// Either payload or validateToken must be provided
type ValidateTokenFn = (token: string) => Promise<TypedUser | null>
```

**Endpoints:**

- `GET /` - Server info
- `POST /rpc` - RPC endpoint (handles HTTP batch)
- `GET /rpc` - WebSocket upgrade
- `GET /rpc/health` - Health check

### `createRpcMiddleware(options)`

Creates Hono middleware for mounting on existing apps.

```typescript
interface RpcMiddlewareOptions {
  adapter: BaseDatabaseAdapter
  payload?: Payload // For Payload auth
  validateToken?: ValidateTokenFn // For custom auth
}
```

### `DatabaseRpcTarget`

The public RPC target class. Use directly for custom setups.

```typescript
import { DatabaseRpcTarget } from '@dotdo/db-rpc-server'

// With Payload auth
const target = new DatabaseRpcTarget(adapter, payload)

// With custom auth
const target = new DatabaseRpcTarget(adapter, undefined, validateTokenFn)
```

### `AuthenticatedDatabaseTarget`

The authenticated RPC target. Returned by `authenticate()`.

## Supported Operations

All Payload database adapter operations are supported over RPC:

### Collections

- `find`, `findOne`, `create`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany`
- `count`, `upsert`, `findDistinct`, `queryDrafts`

### Globals

- `findGlobal`, `createGlobal`, `updateGlobal`

### Versions

- `findVersions`, `createVersion`, `updateVersion`, `deleteVersions`, `countVersions`
- `findGlobalVersions`, `createGlobalVersion`, `updateGlobalVersion`, `countGlobalVersions`

### Transactions

- `beginTransaction`, `commitTransaction`, `rollbackTransaction`

### Jobs

- `updateJobs`

## Migrations

Migrations are **not supported** over RPC. Run migrations directly on the server where the database adapter is configured.

## Security Considerations

1. **Always use HTTPS/WSS in production** - Tokens are sent in RPC calls
2. **Token expiration** - Use short-lived tokens and implement refresh
3. **Rate limiting** - Consider adding rate limiting middleware
4. **Access control** - The authenticated user's permissions apply to all operations

## Related

- [@dotdo/db-rpc](https://www.npmjs.com/package/@dotdo/db-rpc) - The client adapter
- [capnweb](https://github.com/cloudflare/capnweb) - The underlying RPC library
- [Hono](https://hono.dev) - The web framework used for HTTP handling
- [Payload Docs](https://payloadcms.com/docs)
