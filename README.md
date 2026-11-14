# Ley - Event Streaming Engine

Ley is a minimal, single-node, in-memory, log-based messaging broker. Producers publish JSON messages over HTTP. Consumers pull messages by offset and track their own progress.

## Documentation

Versioned architecture, decisions, and build history live in [`docs/`](./docs/README.md). Start with [V0.1](./docs/v0.1/architecture.md) for the broker foundation and [V0.2](./docs/v0.2/architecture.md) for the consumer retry runtime.

## V 0.1 - Architecture Design

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/88be14f8-39f7-41df-b971-eb824480eed5" />

## Technology stack

- TypeScript (ES modules)
- Node.js
- Express 5
- Zod
- Vitest and Supertest for tests
- tsx for local development

## Installation

Requirements: Node.js and pnpm.

```bash
pnpm install
```

## Running the applications

Start the broker:

```bash
pnpm dev:broker
```

The broker listens on `http://localhost:3000` by default. Set `PORT` to use a different port.

In separate terminals, run the example applications:

```bash
pnpm dev:order
pnpm dev:notification
```

`order-service` publishes one example message. `notification-serivce` fetches one batch (limit 10) starting at offset 0, processes messages sequentially, and advances its offset only after successful processing. Both examples are one-shot applications, not long-running pollers.

## API

Base URL: `http://localhost:3000`

### `GET /health`

Returns broker health.

```json
{ "status": "ok" }
```

### `POST /api/v1/messages`

Publishes a message.

Request body:

```json
{
  "key": "order-1234",
  "value": {
    "event_type": "order.created",
    "payload": {
      "order_id": "1234",
      "user_id": "user_123",
      "amount": 1000,
      "currency": "INR"
    }
  },
  "timestamp": {
    "created_at": "2026-01-01T00:00:00.000Z"
  },
  "headers": {
    "source": "order-service",
    "content_type": "application/json"
  }
}
```

Message requirements:

- `key`: string
- `value.event_type`: string
- `value.payload`: non-empty object
- `timestamp.created_at`: ISO 8601 datetime string
- `headers.source` and `headers.content_type`: strings

Success response (`201`):

```json
{
  "offset": 0,
  "message": "Message published successfully"
}
```

Errors:

- `400` for malformed JSON or an invalid message structure.

### `GET /api/v1/messages?offset=0&limit=10`

Fetches messages starting at `offset`.

- `offset`: optional, defaults to `0`, must be a non-negative integer.
- `limit`: optional, defaults to `10`, must be a positive integer.

Success response (`200`):

```json
{
  "messages": [
    {
      "key": "order-1234",
      "value": {
        "event_type": "order.created",
        "payload": {
          "order_id": "1234",
          "user_id": "user_123",
          "amount": 1000,
          "currency": "INR"
        }
      },
      "timestamp": {
        "created_at": "2026-01-01T00:00:00.000Z"
      },
      "headers": {
        "source": "order-service",
        "content_type": "application/json"
      }
    }
  ],
  "offset": 0,
  "limit": 10
}
```

An empty log, or an offset at or beyond the end of the log, returns an empty `messages` array.

Errors:

- `400` for an invalid offset or limit.

Error response shape:

```json
{
  "status": "fail",
  "message": "Invalid message structure"
}
```

Unknown endpoints return `404`.

## Example publish and fetch requests

Publish:

```bash
curl -X POST http://localhost:3000/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{
    "key": "order-1234",
    "value": {
      "event_type": "order.created",
      "payload": { "order_id": "1234", "user_id": "user_123" }
    },
    "timestamp": { "created_at": "2026-01-01T00:00:00.000Z" },
    "headers": {
      "source": "order-service",
      "content_type": "application/json"
    }
  }'
```

Fetch:

```bash
curl "http://localhost:3000/api/v1/messages?offset=0&limit=10"
```

## Testing

```bash
pnpm test
pnpm test:unit
pnpm test:integration
pnpm typecheck
```

Tests do not start the Express server. Integration tests use the existing Express app directly through Supertest.

## Known limitations

- Single node and in-memory only. Restarting the broker loses all messages.
- No disk persistence, replication, or durability guarantees.
- No consumer groups, subscriptions, or broker-managed offsets. Consumers track their own offsets.
- Pull-based consumption only; there is no push delivery.
- No message deletion, retention, compaction, or TTL.
- No authentication, authorization, TLS, or rate limiting.
- No delivery guarantee across process crashes.
- Not safe for concurrent multi-process access.
- Example clients assume the broker is at `http://localhost:3000`. Broker unavailability is caught and logged by the example clients.
- Consumer progress is not stored by the broker, so restarting a consumer loses its in-memory progress.
