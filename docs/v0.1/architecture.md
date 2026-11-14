# Ley V0.1 — Broker Architecture

> Historical record. This document describes the broker as it existed in V0.1, before the
> consumer retry system and ordering guarantees introduced in V0.2.

## Overview

Ley V0.1 is a single-process HTTP broker. Messages are appended to an in-memory array and read by numeric offset. There is no database, disk log, or external infrastructure.

## Components

- `src/broker/server.ts`: starts the Express listener on `PORT` (default `3000`).
- `src/broker/app.ts`: builds the Express app, enables JSON parsing, mounts routes and health, and registers 404 and error middleware.
- `src/broker/route.ts`: mounts the message router at `/api/v1/messages`.
- `src/broker/modules/message/message.routes.ts`: maps `POST` and `GET` to controller functions.
- `src/broker/modules/message/message.controller.ts`: parses query parameters, applies defaults, calls the broker service, and writes HTTP responses.
- `src/broker/modules/broker/broker.service.ts`: validates messages with Zod, assigns offsets, appends messages, and fetches batches.
- `src/broker/modules/message/message.schema.ts`: defines the accepted message structure.
- `src/broker/modules/storage/memory-log.ts`: owns the in-memory message array and exposes `append`, `read`, and `length`.
- `src/shared/errors/appError.ts`: represents operational errors with an HTTP status code.
- `src/middlewares/error.middlewares.ts`: converts application and JSON parse errors into consistent HTTP responses.
- `src/middlewares/not-found.middlewares.ts`: returns `404` for unknown routes.
- `examples/order-service` and `examples/notification-serivce`: example producer and consumer clients.

## Message lifecycle

1. A producer creates a message matching the message contract.
2. The producer sends it to `POST /api/v1/messages`.
3. Express parses the JSON body.
4. The broker service validates the message with Zod.
5. The broker assigns the next offset using the current log length.
6. The memory log appends a structured clone of the message.
7. The broker returns the assigned offset with HTTP `201`.
8. Consumers read the message by offset through `GET /api/v1/messages`.
9. The broker does not delete messages, so they remain readable until the process exits.

## Producer-consumer flow

Producers publish messages; they do not receive consumer acknowledgements.

Consumers pull batches by supplying `offset` and `limit`. A new consumer starts at offset `0`. It processes messages sequentially and advances its local offset only after a message is processed successfully. If processing fails, the consumer does not advance, so the same message is fetched again on retry.

The broker stores no consumer identity or committed offset. Consumer progress exists only in the consumer process. Restarting a consumer resets its in-memory offset.

## In-memory storage design

The log is a module-level array:

```ts
let messages: Message[] = [];
```

- The array index is the message offset.
- `append` pushes a `structuredClone` of the validated message.
- `read` validates `offset` and `limit`, then returns `structuredClone(messages.slice(offset, offset + limit))`.
- `length` returns the current number of messages.

Because the broker is single-process and operations are synchronous, offsets are assigned sequentially and insertion order is preserved. There is no deletion or eviction API.

The clones on append and read prevent callers from mutating stored messages through external references. Fetching a message twice returns two independent copies.

All state lives in process memory. A broker restart creates a new empty array, so all messages are lost. Tests reset the module registry between cases to get isolated log state.

## Error handling

Controllers and services throw `AppError` for invalid input. The error middleware maps operational errors to `{ status, message }` with the appropriate HTTP status. Malformed JSON is translated into a `400` response. Unknown routes are handled by the not-found middleware with `404`. Unexpected errors return `500` and are logged internally.

## HTTP behaviour

- `POST /api/v1/messages` returns `201` with `{ offset, message }`.
- `GET /api/v1/messages` returns `200` with `{ messages, offset, limit }`.
- Defaults are `offset=0` and `limit=10`.
- An empty log or an offset beyond the log length returns an empty `messages` array.
- Invalid offset or limit values return `400`.
- Unknown routes return `404`.

## Known limitations in V0.1

- Single node, in-memory only. Broker restart loses all messages.
- No persistence, replication, or durability guarantees.
- No consumer groups, subscriptions, or broker-managed offsets.
- Pull-based consumption only.
- No message deletion, retention, compaction, or TTL.
- No authentication, authorization, TLS, or rate limiting.
- No retry handling, dead-letter queue, or graceful shutdown coordination in the example consumer.
