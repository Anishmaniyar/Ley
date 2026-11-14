# Ley V0.2 — Decisions

This is the decision record for the V0.2 consumer failure handling, retry, shutdown, and
ordering work. Each decision states the context, the choice, the alternatives, and the
consequences.

## 1. Consumer-managed retries

**Context.** The broker is a simple append-only log with no delivery state.

**Decision.** Retries live entirely in the consumer. The broker does not know about
attempts, failures, or dead letters.

**Alternatives.**

- Broker-managed retries: requires the broker to track consumer identity, delivery
  state, and retry queues.
- No retries: forces every consumer to reimplement failure handling.

**Consequences.**

- The broker stays small and stateless with respect to consumers.
- Retry state is volatile and lost on consumer restart.
- Different consumers can choose different retry policies.

## 2. Separate retry scheduler

**Context.** A failed message may need to wait 20 seconds before its next attempt.

**Decision.** Run retries in a separate scheduler loop rather than sleeping inside the
consumer batch loop.

**Alternatives.**

- Sleep inside the consumer loop: blocks new message processing during backoff.
- Immediate retries: ignores backoff and amplifies downstream failures.

**Consequences.**

- New messages keep flowing while retries wait.
- Two execution paths must share one concurrency control, which is solved by the mutex.

## 3. Min-heap priority queue

**Context.** Retries have different deadlines and must run in deadline order.

**Decision.** Store retry entries in a binary min-heap ordered by `nextRetryAt`.

**Alternatives.**

- FIFO queue: a message due in 3 seconds can be stuck behind one due in 20 seconds.
- Sorted array: correct, but insertion and removal are O(n) instead of O(log n).
- Scan the whole queue on every tick: O(n) work even when nothing is due.

**Consequences.**

- `peek`, `push`, and `pop` are O(log n).
- The earliest retry is always at the root.

## 4. Absolute `nextRetryAt` deadlines

**Context.** Retries need to wait for varying amounts of time.

**Decision.** Store an absolute timestamp per entry and sleep until that deadline.

**Alternatives.**

- Store a relative delay and decrement it: requires repeated wake-ups and bookkeeping.
- Fixed polling interval: wakes up even when nothing is due.

**Consequences.**

- The scheduler can sleep for exactly the remaining time.
- The wake-up signal can interrupt the sleep when a new earlier retry is added.

## 5. Wake-up signal

**Context.** The scheduler must not busy-poll an empty retry queue.

**Decision.** Use a small notification primitive that supports timeout-based waits and
retains a notification that arrives before a wait begins.

**Alternatives.**

- `setInterval` polling: wastes cycles.
- A plain Promise per wait: a notification sent before the wait can be lost.

**Consequences.**

- Empty queue: wait indefinitely until notified.
- Non-empty queue: wait until the deadline or a new notification.
- Spurious wake-ups are harmless because the scheduler re-inspects the queue.

## 6. Exponential backoff with jitter

**Context.** Immediate retries hammer a failing dependency; synchronized retries create
traffic spikes.

**Decision.** Delay the next attempt with exponential backoff and apply jitter around
the calculated delay, then clamp to the maximum.

**Alternatives.**

- Fixed delay: predictable but can be too aggressive or too slow.
- No jitter: many messages that failed together retry together.
- Unlimited backoff: an unbounded wait.

**Consequences.**

- Delays grow: 2 s, 4 s, 8 s, 16 s, then 20 s at the cap.
- Jitter is 30% in the current configuration.
- The final delay never exceeds 20 seconds.

## 7. Retry-count semantics

**Context.** `MAX_RETRIES` could be interpreted as total attempts or retries after the
initial attempt.

**Decision.** `MAX_RETRIES = 5` means five retries after the initial attempt, for six
total attempts. `retryCount` stores the number of retries already attempted.

**Alternatives.**

- Count the initial attempt in the limit: would allow only four retries.

**Consequences.**

- Attempt 1 is the initial publish/processing.
- Attempts 2–6 are the five retries.
- The sixth failure moves the message to the dead-letter list.

## 8. In-memory dead-letter list

**Context.** Exhausted messages must not silently disappear, but V0.2 has no
persistence.

**Decision.** Keep exhausted `RetryEntry` objects in an in-memory list exposed through
`getDeadLetters()`.

**Alternatives.**

- Drop exhausted messages: loses diagnostic information.
- Persist a dead-letter queue: adds storage and recovery concerns outside V0.2.
- Block the consumer forever: stalls all progress.

**Consequences.**

- Operators can inspect exhausted messages in-process.
- The list is lost on restart.
- A durable dead-letter queue is deferred to a later version.

## 9. One shared async mutex

**Context.** New messages and due retries are produced by two different loops.

**Decision.** Create one mutex in `index.ts` and expose one `processMessageSafely`
function to both loops.

**Alternatives.**

- One mutex per loop: new messages and retries could process simultaneously.
- No mutex: concurrent processing of the same consumer's work.

**Consequences.**

- Global processing concurrency is exactly one.
- The scheduler stays unaware of concurrency control.
- A slow message delays a due retry, and a slow retry delays new messages; this is the
  accepted trade-off for deterministic serialization.

## 10. Injected processing function

**Context.** The scheduler needs to execute work, but should not depend on the mutex,
the broker client, or domain logic.

**Decision.** The scheduler accepts a `processMessage` function from the caller.

**Alternatives.**

- Import the processor directly: tight coupling and hard to test.
- Let the scheduler own the mutex: duplicates concurrency policy.

**Consequences.**

- The scheduler is reusable and easy to test.
- The application decides how processing is serialized.

## 11. Structured error data

**Context.** Raw `Error` objects do not serialize cleanly and may carry large stacks.

**Decision.** Store `{ name, message, stack? }` in `RetryEntry.lastError`.

**Alternatives.**

- Store the raw `Error`: poor serialization and coupling.
- Store only a string: loses the error type.

**Consequences.**

- Errors are inspectable and loggable.
- Every retry updates `lastError` with the most recent failure.

## 12. Cancel queued mutex tasks during shutdown

**Context.** A shutdown can happen while one task holds the mutex and another waits.

**Decision.** `runExclusive` accepts an `AbortSignal` and checks it before starting a
queued task.

**Alternatives.**

- Let every queued task run: shutdown can take much longer than necessary.
- Cancel running tasks: risks interrupting work halfway through.

**Consequences.**

- Work that has not started is rejected with `AbortError`.
- Work that is already executing finishes normally.
- The consumer does not advance the offset for cancelled work.

## 13. Finish in-flight work

**Context.** Abruptly terminating processing can leave side effects in an unknown state.

**Decision.** Shutdown finishes the task that currently holds the mutex, then stops.

**Alternatives.**

- Abort immediately: faster shutdown but unsafe for domain operations.

**Consequences.**

- Shutdown latency is bounded by the current message's processing time.
- `installGracefulShutdown` forces exit after 10 seconds as a backstop.

## 14. Stop the scheduler concurrently with the consumer drain

**Context.** Earlier shutdown ordering stopped the retry scheduler only after the
consumer loop finished. During that window a retry could be dequeued, queue behind the
mutex, be cancelled by the abort signal, then be counted and rescheduled as if it had
failed.

**Decision.** `stop()` aborts the shutdown signal and calls `retryScheduler.stop()` at
the same time as it waits for the consumer loop (`Promise.all`).

**Alternatives.**

- Wait for the consumer loop first: leaves the race described above.
- Never cancel retries: shutdown can hang behind long backoffs.

**Consequences.**

- A retry cancelled during shutdown is never counted or re-enqueued.
- The race is covered by an integration test.

## 15. Catch scheduler loop rejections

**Context.** `RetryScheduler.start()` previously stored the loop promise without a
rejection handler. An unexpected loop error would become an unhandled rejection.

**Decision.** Attach a rejection handler that stores the error; `stop()` rethrows it so
shutdown reports the failure.

**Alternatives.**

- Ignore loop errors: hides a crash.
- Let them become unhandled rejections: can terminate the process unpredictably.

**Consequences.**

- No unhandled rejection from the scheduler loop.
- Unexpected scheduler failures surface during shutdown.

## 16. Append order defines the total order

**Context.** Concurrent producers can publish messages whose creation timestamps do not
match delivery order.

**Decision.** Define the total order as broker append order. Offset assignment and
insertion happen in one synchronous block. Creation timestamps are metadata only.

**Alternatives.**

- Sort by `timestamp.created_at`: requires synchronized clocks and still leaves ties.
- Promise causal order across producers: requires coordination the broker does not have.
- Promise per-key causal order: not enforceable without producer-side sequencing.

**Consequences.**

- Every accepted message gets a unique, gap-free offset.
- Cross-producer order reflects delivery and parsing order, not causality.
- Per-key order is only the projection of append order.
- Producers that need causal per-key order must serialize their own publishes.
- The guarantee is single-process and depends on the publish path staying synchronous.

## 17. Simulated 40% processing failure

**Context.** The example processor originally never failed, so retry, backoff,
dead-letter, and shutdown paths were difficult to exercise.

**Decision.** Make `processMessage` fail 40% of the time by default, with injectable
`failureRate`, `delayMs`, and `random` options for deterministic tests.

**Alternatives.**

- Keep the processor always successful: retry paths stay untested.
- Hard-code failure without injection: tests become nondeterministic.

**Consequences.**

- The running example exercises retry behavior.
- Tests can force success or failure deterministically.
- The 40% rate is a test/demo behaviour, not a production policy.

## 18. Volatile state in V0.2

**Context.** Durable offsets, retry queues, and dead letters require storage and
recovery design.

**Decision.** Keep all consumer and retry state in memory for V0.2.

**Alternatives.**

- Add persistence now: expands scope into storage engines and recovery.

**Consequences.**

- Restarting a consumer resets its offset and loses pending retries.
- Messages can be replayed from the broker, so duplicates are possible.
- Exactly-once processing is not provided.
- Durability is deferred to a later version.
