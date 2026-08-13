# cl-event-sourcing-kit

`cl-event-sourcing-kit` is a domain-independent event sourcing protocol for
Common Lisp. It defines the consistency boundary around an event stream while
leaving storage, serialization, transactions, and business policy to an
  backend or application.

The core keeps event payloads and metadata opaque. The optional systems add a
reference in-memory store, synchronous projections, and a recoverable durable
runtime without changing the core protocol.

!!! note "Reference runtime boundary"

    The durable system is a portable reference runtime. It is not a
    distributed database, message broker, scheduler, or high-availability
    deployment.

## Systems

| System | Provides |
| --- | --- |
| `cl-event-sourcing-kit` | Immutable event envelopes, store protocol, replay, staging, conditions, and CPS entry points. |
| `cl-event-sourcing-kit/in-memory` | A reference in-memory event store with optimistic concurrency, idempotency, snapshots, and global positions. |
| `cl-event-sourcing-kit/projection` | Synchronous projection construction, rebuild, and bounded catch-up. |
| `cl-event-sourcing-kit/durable` | Safe serialization, a recoverable file store, subscriptions, outbox APIs, checkpoint persistence, and schema evolution helpers. |

## Where to start

- [Getting started](getting-started.md) — load a system and run the smallest
  append/replay example.
- [Core concepts](guide/core-concepts.md) — understand the event envelope and
  the storage-independent boundary.
- [Event store](guide/event-store.md) — implement or evaluate a backend.
- [Replay and staging](guide/replay-and-staging.md) — load aggregates and
  build command-side sessions.
- [Durable runtime](guide/durable-runtime.md) — use the optional reference
  persistence and delivery components.
- [Capability matrix](project/capability-matrix.md) — distinguish library
  guarantees from production backend and deployment responsibilities.
- [API reference](reference/api.md) — find the public protocol grouped by
  system.

The repository is intentionally small at the boundary: a production backend
owns its database transaction, locking, serialization, recovery, retention,
and deployment guarantees.
