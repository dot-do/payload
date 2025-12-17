# @dotdo/db-rpc

RPC client database adapter for Payload CMS. Connects to a remote `@dotdo/db-rpc-server` over HTTP or WebSocket.

## Installation

```bash
pnpm add @dotdo/db-rpc
```

## Usage

```typescript
import { buildConfig } from 'payload'
import { rpcAdapter } from '@dotdo/db-rpc'

export default buildConfig({
  db: rpcAdapter({
    url: 'https://db-server.example.com/rpc',
    token: process.env.DB_RPC_TOKEN,
  }),
  // ... rest of config
})
```

## Configuration Options

| Option      | Type                                        | Default  | Description                                                                             |
| ----------- | ------------------------------------------- | -------- | --------------------------------------------------------------------------------------- |
| `url`       | `string`                                    | -        | RPC server URL. Use `http://` or `https://` for HTTP, `ws://` or `wss://` for WebSocket |
| `token`     | `string \| () => string \| Promise<string>` | -        | Bearer token for authentication. Can be a string or a function that returns a token     |
| `transport` | `'http' \| 'websocket'`                     | `'http'` | Transport type for RPC communication                                                    |

## Transport Types

### HTTP (Default)

Uses HTTP batch mode via [capnweb](https://github.com/cloudflare/capnweb). Multiple RPC calls are batched into a single HTTP request, making it efficient for typical request/response patterns.

```typescript
rpcAdapter({
  url: 'https://db-server.example.com/rpc',
  token: 'your-token',
  transport: 'http', // default
})
```

### WebSocket

Uses a persistent WebSocket connection. Better for real-time or high-frequency operations where connection overhead matters.

```typescript
rpcAdapter({
  url: 'wss://db-server.example.com/rpc',
  token: 'your-token',
  transport: 'websocket',
})
```

## Authentication

The adapter uses bearer token authentication. The token is validated by the server using Payload's built-in auth system, supporting:

- **JWT tokens** - Obtained from `payload.login()`
- **API keys** - If configured in your Payload users collection

### Static Token

```typescript
rpcAdapter({
  url: 'https://db-server.example.com/rpc',
  token: process.env.DB_RPC_TOKEN,
})
```

### Dynamic Token (for token refresh)

```typescript
rpcAdapter({
  url: 'https://db-server.example.com/rpc',
  token: async () => {
    // Fetch fresh token from your auth service
    const response = await fetch('/api/get-db-token')
    const { token } = await response.json()
    return token
  },
})
```

## Limitations

- **Migrations**: Cannot be run over RPC. Run migrations directly on the server.
- **Direct Database Access**: No raw database queries. All operations go through Payload's adapter interface.

## How It Works

1. Client calls `payload.create()`, `payload.find()`, etc.
2. The RPC adapter serializes the call using [capnweb](https://github.com/cloudflare/capnweb)
3. Request is sent to the server over HTTP or WebSocket
4. Server validates the token, executes the operation on the underlying database
5. Result is serialized and returned to the client

## Related

- [@dotdo/db-rpc-server](https://www.npmjs.com/package/@dotdo/db-rpc-server) - The server component
- [capnweb](https://github.com/cloudflare/capnweb) - The underlying RPC library
- [Payload Docs](https://payloadcms.com/docs)
