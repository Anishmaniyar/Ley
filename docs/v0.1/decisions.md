# Ley V0.1 — Decisions

> Historical record. These are the decisions that shaped the V0.1 broker, before the
> V0.2 consumer retry and ordering work.

## Why an in-memory array?

V0.1 focuses on the core log abstraction: append messages, assign offsets, and read them back in order. A JavaScript array provides that behaviour directly, with no setup, dependencies, or external services. Array indexes naturally represent offsets, and `slice` provides offset-based batch reads.

The trade-off is that all messages are lost when the process exits and memory usage grows without bound. This is acceptable for V0.1 and keeps the implementation small.

## Why offset-based retrieval?

Offsets give consumers a simple, stable cursor into an append-only log. A consumer can resume from any position, fetch a bounded batch, and avoid receiving earlier messages again. Offsets also make ordering explicit: message `0` was appended before message `1`.

Because the log is append-only in V0.1, an offset remains meaningful for the lifetime of the process. The broker does not need to track which consumer read what.

## Why pull-based consumption?

Pull-based consumption lets each consumer control its own pace and batch size. The broker does not need to manage connections, delivery state, retries, or backpressure for each consumer. This is simpler and fits the HTTP request/response model already used by the broker.

The trade-off is higher latency than push delivery and more work for consumers, which must poll or schedule their own fetches.

## Why separate producer and consumer applications?

Producers and consumers have different responsibilities and lifecycles. Keeping them separate demonstrates that the broker is the shared boundary: producers only publish, consumers only fetch, and neither depends on the other's implementation. It also allows either side to restart independently.

In V0.1, the examples are separate one-shot applications. The broker stores no consumer state, so a restarted consumer starts again from its own chosen offset.

## Why no persistence in V0.1?

Persistence introduces storage engines, file formats, recovery logic, retention, and failure handling. None of that is required to validate the core log and HTTP contract. In-memory storage keeps the first version easy to understand and test.

The explicit limitation is that a broker restart loses all messages and there is no durability guarantee across process crashes. Persistence and recovery are deferred to a later version.
