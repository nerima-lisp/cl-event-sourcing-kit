# API reference

The public API is grouped by ASDF system. The complete export list is kept in
`src/package.lisp`; the groups below are the stable concepts a backend or
application should depend on.

## Core system

### Event envelope

`domain-event`, `domain-event-p`, `make-domain-event`, and the
`domain-event-*` readers create and inspect immutable event envelopes.

### Conditions

The core exports structured conditions for invalid envelopes and versions,
version conflicts, duplicate IDs, unsupported operations, invalid snapshots,
retention gaps, and projection failures. Conditions carry the relevant event,
stream, operation, or checkpoint values for programmatic handling.

### Store protocol

These generic operations define the backend protocol directly:

```text
event-store-append
event-store-read
event-store-read-all
event-store-current-version
event-store-current-global-position
event-store-stream-exists-p
event-store-global-position-supported-p
event-store-event-equivalent-p
event-store-append-batch
event-store-save-snapshot
event-store-read-snapshot
event-store-delete-snapshot
event-store-snapshots-supported-p
event-store-prune
event-store-retention-supported-p
event-store-retention-floor
```

`event-snapshot`, `make-event-snapshot`, `event-append-request`,
`make-event-append-request`, and `event-append-result` are the corresponding
value objects.

### Replay and staging

`replay-events`, `load-aggregate`, `upcast-event`, `upcast-events`,
`event-staging`, `make-event-staging`, `stage-event`, `uncommitted-events`,
and `commit-events` provide pure reconstruction and a small command-side
session.

### CPS entry points

The core also exports synchronous continuation-shaped entry points:
`event-store-append/cc`, `event-store-append-batch/cc`, `event-store-read/cc`,
`replay-events/cc`, `commit-events/cc`, `rebuild-projection/cc`, and
`advance-projection/cc`. Each accepts success and error continuations while
preserving the validation and transaction semantics of its synchronous
counterpart. `define-cps-operation` is the exported macro for defining an
operation with the same boundary.

## In-memory system

`make-event-store` creates the reference in-memory implementation. It supports
stream and global-feed reads, optimistic expected versions, all-or-nothing
append batches, duplicate-ID idempotency, snapshots, and global positions.

Use the in-memory system to test backend-independent application behavior; do
not infer production durability from it.

## Projection system

`projection`, `make-projection`, `rebuild-projection`, and
`advance-projection` provide synchronous global-feed projection processing.
`projection-state`, `projection-checkpoint`, and `projection-handler` expose
the projection state and behavior.

## Durable system

The durable exports include:

- `event-serializer`, `make-event-serializer`, and event/snapshot serialization
  functions;
- `file-event-store`, `make-file-event-store`,
  `recover-file-event-store`, `file-event-store-sync`, and
  `close-file-event-store`;
- subscription offset stores, `make-subscription`, `subscription-poll`,
  `subscription-ack`, and `deliver-subscription`;
- outbox messages and stores, `outbox-append`, `outbox-claim`,
  `outbox-ack`, `outbox-fail`, `outbox-requeue`, `outbox-dispatch`, and
  `event-store-append-with-outbox`;
- projection checkpoint stores, `make-durable-projection-runner`, and
  `run-projection-once`;
- `upcaster-registry`, `register-upcaster`, and `make-observed-event-store`;
  retry policy is supplied directly by `cl-resilience-kit` via
  `resilience-kit:with-retry`.

Consult [Durable runtime](../guide/durable-runtime.md) for guarantees and
limitations rather than treating these reference implementations as a
distributed service.

## ASDF systems

| System | Depends on | Main additions |
| --- | --- | --- |
| `cl-event-sourcing-kit` | `cl-boundary-kit` | Core protocol, replay, staging, conditions, CPS. |
| `cl-event-sourcing-kit/in-memory` | Core, `cl-concurrent-kit` | Reference event store. |
| `cl-event-sourcing-kit/projection` | Core | Projection and rebuild. |
| `cl-event-sourcing-kit/durable` | In-memory, projection, `cl-concurrent-kit`, `cl-resilience-kit` | File persistence, delivery, outbox, checkpoints, evolution. |
