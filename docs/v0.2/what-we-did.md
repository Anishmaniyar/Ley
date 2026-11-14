# Ley V0.2 — What We Built

This is the feature-by-feature history of V0.2. It records what was built, why it was
built, which files own each responsibility, and how the behaviour was verified. The
current architecture is described in [`architecture.md`](./architecture.md); the
decision rationale is in [`decisions.md`](./decisions.md).

## 1. Starting point: V0.1

V0.1 delivered a single-process, in-memory HTTP broker:

- Producers publish JSON messages over `POST /api/v1/messages`.
- Consumers pull messages over `GET /api/v1/messages?offset=&limit=`.
- Offsets are array indexes.
- The broker keeps no consumer state.
- The example consumer fetched one batch, processed it, and exited.

V0.2 keeps that broker contract and adds a real consumer runtime around it.

## 2. V0.2 feature log

### Feature 1 — Atomic offset assignment in the broker

**What we built.** `append` now assigns the offset and inserts the message in one call,
returning the assigned offset. `publishMessage` returns that offset directly instead of
reading the length separately.

**Why.** Offset assignment and insertion are one logical operation. Keeping them in one
synchronous block means a concurrent publish cannot observe or reuse an offset between
the read and the push.

**Files.**

- `src/broker/modules/storage/memory-log.ts`
- `src/broker/modules/broker/broker.service.ts`

**Verified by.** `tests/integration/concurrency.test.ts` (unique, continuous offsets
under 100 overlapping publishes) and `tests/unit/broker.test.ts`.

### Feature 2 — Long-running consumer with a local offset

**What we built.** The notification consumer now polls the broker in a loop, fetches
batches of 10, processes messages sequentially, and advances a local offset after
successful processing.

**Why.** A real consumer must keep consuming, not exit after one batch. Progress stays
consumer-owned because the broker deliberately has no consumer state.

**Files.**

- `examples/consumer/notification-service/index.ts`
- `examples/consumer/notification-service/offset/offset-manager.ts`
- `examples/consumer/shared/broker-client.ts`

**Verified by.** `tests/integration/consumer.test.ts` and the existing message API tests.

### Feature 3 — Shared async mutex

**What we built.** A small mutex serializes tasks. New-message processing and retry
processing share one instance, so global processing concurrency is one.

**Why.** A new message and a due retry must never run at the same time. One shared
mutex is simpler than coordinating two independent locks.

**Files.**

- `examples/consumer/shared/async-mutex.ts`
- `examples/consumer/notification-service/index.ts`

**Verified by.** `tests/unit/consumer-components.test.ts` (sequencing, failure
isolation, original error propagation) and the "never processes a new message and a due
retry at the same time" integration test.

### Feature 4 — Retry entry and deadline-ordered retry queue

**What we built.** A `RetryEntry` captures the message, retry count, next deadline, and
last error. `RetryQueue` wraps a generic min-heap ordered by `nextRetryAt`.

**Why.** Retries must run in deadline order. A FIFO queue would let a retry due in 3
seconds wait behind one due in 20 seconds.

**Files.**

- `examples/consumer/notification-service/retry/retry-entry.ts`
- `examples/consumer/notification-service/retry/retry-queue.ts`
- `examples/consumer/shared/min-heap.ts`

**Verified by.** Unit tests for heap ordering, duplicates, removal, and retry queue
deadline order.

### Feature 5 — Wake-up signal

**What we built.** A notification primitive with timeout support that remembers a
notification delivered before a wait begins.

**Why.** The scheduler must sleep without busy-polling, but a newly scheduled earlier
retry must interrupt that sleep. A plain promise would lose a notification sent before
the wait.

**Files.**

- `examples/consumer/shared/wake-up_signal.ts`

**Verified by.** Unit tests for notify-before-wait, notify-while-waiting, timeout
without notification, and timer cleanup.

### Feature 6 — Retry scheduler

**What we built.** A dedicated scheduler loop that peeks the earliest retry, sleeps
until its deadline, dequeues and processes it through the shared processing function,
then either completes it, reschedules it, or dead-letters it.

**Why.** Backoff delays must not block normal message polling. The scheduler also keeps
retry ordering and retry limits in one place.

**Files.**

- `examples/consumer/notification-service/retry/retry-scheduler.ts`

**Verified by.** Unit tests for deadline waiting, retrying failed messages, exhaustion,
refusing new entries before start and after stop, and shutdown cancellation; plus
integration tests for scheduling, success, and rescheduling.

### Feature 7 — Exponential backoff, jitter, and delay cap

**What we built.** `calculateDelay` applies exponential backoff, jitter, and a maximum
delay, with input validation.

**Why.** A failing dependency needs time to recover, and synchronized retries create a
second traffic spike.

**Files.**

- `examples/consumer/notification-service/retry/retry-delay.ts`
- `examples/consumer/notification-service/config.ts`

**Verified by.** Unit tests for the backoff sequence, lower and upper jitter bounds, the
maximum-delay cap, and invalid input.

### Feature 8 — Retry limits and dead-letter handling

**What we built.** `MAX_RETRIES = 5` means five retries after the initial attempt.
Exhausted entries leave the retry queue and go to an in-memory dead-letter list exposed
through `getDeadLetters()`. `lastError` is updated on every failed retry.

**Why.** Retrying forever can hide a permanent failure. Dropping exhaustions silently
loses diagnostics.

**Files.**

- `examples/consumer/notification-service/retry/retry-scheduler.ts`
- `examples/consumer/notification-service/retry/retry-entry.ts`

**Verified by.** Unit tests for five attempts then dead-letter, retry-count and
`lastError` updates, and integration tests for rescheduling.

### Feature 9 — Graceful shutdown and cancellation

**What we built.** Shutdown stops new fetches, aborts in-flight waits, finishes the
active message, cancels work still waiting for the mutex, stops scheduling retries, and
finishes an active retry. A 10-second watchdog forces exit if shutdown hangs.

**Why.** In-memory state makes abrupt termination wasteful, and interrupting an active
side effect leaves it in an unknown state.

**Files.**

- `examples/consumer/notification-service/index.ts`
- `examples/consumer/shared/shutdown.ts`
- `examples/consumer/shared/async-mutex.ts`
- `examples/consumer/notification-service/retry/retry-scheduler.ts`

**Verified by.** Integration tests for shutdown during normal processing, shutdown
during retry processing, cancellation of a retry queued behind the mutex, and unit
tests for mutex cancellation.

### Feature 10 — Deterministic failure simulation for testing

**What we built.** `processMessage` fails 40% of the time by default and accepts
injected `delayMs`, `failureRate`, and `random` values.

**Why.** The original processor never failed, so retry, backoff, dead-letter, and
shutdown paths were not exercised by the example consumer.

**Files.**

- `examples/consumer/notification-service/processing/message-processor.ts`

**Verified by.** Unit tests for the success branch, failure branch, and failure-rate
boundary.

### Feature 11 — Consumer test suites

**What we built.** Two focused test files were added: one for individual components and
one for integration behaviour.

**Files.**

- `tests/unit/consumer-components.test.ts`
- `tests/integration/consumer.test.ts`

**Coverage highlights.**

- Mutex sequencing, failure isolation, original error, cancellation.
- Priority queue insertion, removal, ordering, duplicates.
- Wake-up signal before-wait, while-waiting, timeout.
- Backoff calculation, jitter bounds, cap.
- Retry scheduler waiting, retrying, exhaustion, shutdown.
- Retry queue ordering.
- Processor 60/40 simulation.
- Multiple producers and independent consumers.
- Retry scheduling, success, rescheduling.
- Continued processing after a failure and after a failed retry.
- No simultaneous new-message and retry processing.
- Graceful shutdown during normal and retry work.
- Cancellation of a retry queued behind the mutex.
- Volatile restart behavior.

### Feature 12 — Engineering review hardening

We performed a final V0.2 review and fixed two concrete issues.

1. **Unhandled scheduler rejection.** `RetryScheduler.start()` stored the loop promise
   without a rejection handler. It now catches the error, stores it, and rethrows it
   from `stop()` so shutdown reports it.
2. **Shutdown race.** The scheduler was stopped only after the consumer loop drained.
   A retry queued behind the mutex could be cancelled during that window, then counted
   and rescheduled as a failed attempt. The scheduler is now stopped concurrently with
   the consumer drain, and a regression test covers the race.

**Files.**

- `examples/consumer/notification-service/retry/retry-scheduler.ts`
- `examples/consumer/notification-service/index.ts`
- `tests/integration/consumer.test.ts`

### Feature 13 — Message ordering under concurrency

**What we built.** Explicit ordering rules and tests. The broker's total order is append
order, offset assignment is atomic, timestamps are metadata only, rejected messages
consume no offset, reads only observe prefixes, and per-key order is the projection of
append order.

**Why.** Sequential publishing had an obvious order; concurrent producers require an
explicit definition rather than an accidental one.

**Files.**

- `src/broker/modules/storage/memory-log.ts` (synchronous-block invariant documented)
- `tests/integration/concurrency.test.ts`
- `docs/v0.2/architecture.md` and `docs/v0.2/decisions.md`

**Verified by.** Tests for stable repeated reads, append order versus creation time,
prefix-only reads during concurrent publishing, and per-key projection.

## 3. Verification log

The following was observed on the development machine for this version. Timings vary by
machine and are useful for flakiness detection, not as benchmarks.

| Check | Result |
| --- | --- |
| `pnpm typecheck` | Clean |
| Full suite | 105 tests, 5 files, all passing |
| Full suite repeated | 10/10 runs passed; 4.25–4.97 s per warm run (one cold run ~25 s) |
| Consumer integration repeated | 15/15 runs passed; 2.19–2.89 s per run |
| Concurrency/ordering repeated | 20/20 runs passed; 2.50–3.78 s per run |

## 4. Decisions that changed during the review

| Original plan | Change | Reason |
| --- | --- | --- |
| Stop the scheduler after the consumer loop | Stop both concurrently | Closed the shutdown race that could count a cancelled retry |
| Store the scheduler loop promise directly | Catch and surface the error | Avoided an unhandled promise rejection |
| Keep the processor always successful | Add a 40% failure simulation | Made retry and shutdown paths testable |
| Describe ordering only informally | Add explicit ordering rules and tests | Concurrent publishing needs a precise guarantee |

## 5. Known limitations at the end of V0.2

- All broker, consumer, retry, and dead-letter state is in memory.
- Restarting the consumer resets the offset and loses pending retries.
- Duplicate processing is possible after a restart or a crash between a side effect and
  progress recording.
- Exactly-once processing is not provided.
- The broker is single-process; ordering guarantees do not extend across processes.
- The example consumer hard-codes `http://localhost:3000` for the broker.
- The 40% failure rate in `processMessage` is a demo/testing behaviour.

## 6. Deliberately deferred to later versions

- Durable consumer offsets.
- Persistent retry and dead-letter queues.
- Broker persistence and replication.
- Consumer groups, partition assignment, leases, heartbeats, and fencing tokens.
- Exactly-once processing and idempotency infrastructure.
- Multi-node coordination.

## 7. V0.2 in one paragraph

V0.2 turns the one-shot example consumer into a long-running consumer with a local
offset, a shared mutex that serializes all processing, and a separate retry system built
from a deadline-ordered min-heap, a wake-up signal, and a scheduler that applies
exponential backoff with jitter. Failed messages are scheduled independently so later
messages keep flowing. Exhausted messages move to an in-memory dead-letter list.
Shutdown stops new work, cancels queued work, and finishes active work. The broker's
append order is the single total order, with atomic, gap-free offset assignment and no
use of producer creation timestamps.
