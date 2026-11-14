# Ley Documentation

Documentation is versioned so each release keeps its own architecture, decisions, and
build history. Treat each version folder as a snapshot of what was understood and
implemented at that point, not as the current source of truth for later versions.

## V0.1 — Broker foundation

The single-process, in-memory HTTP broker with pull-based consumers.

- [Architecture](./v0.1/architecture.md)
- [Decisions](./v0.1/decisions.md)

## V0.2 — Consumer failure handling, retry, and ordering

The long-running consumer with a shared mutex, retry scheduler, exponential backoff,
dead-letter handling, graceful shutdown, and explicit ordering rules.

- [Architecture](./v0.2/architecture.md)
- [Decisions](./v0.2/decisions.md)
- [What we built](./v0.2/what-we-did.md)

## Reading order

1. Start with the V0.1 architecture to understand the broker contract.
2. Read the V0.2 architecture for the consumer and retry runtime.
3. Read the V0.2 decisions for the trade-offs behind each choice.
4. Read the V0.2 build history for feature-by-feature implementation and verification.

## Conventions

- `architecture.md` describes components, flows, guarantees, and limitations.
- `decisions.md` records context, choice, alternatives, and consequences.
- `what-we-did.md` records the feature history and verification for that version.
- Diagrams use Mermaid and should be updated when the described flow changes.
