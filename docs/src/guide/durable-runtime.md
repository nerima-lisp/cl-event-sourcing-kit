# Durable runtime

The optional `cl-event-sourcing-kit/durable` system is a composable reference
runtime. It makes the storage and delivery boundaries explicit while keeping
the core protocol independent of a wire format.

## Safe serialization

`make-event-serializer` accepts application `:encode` and `:decode` functions,
and bounds serialized values with `:max-bytes` and `:max-depth`. The default
serializer handles a bounded, safe portable S-expression subset and rejects
unsafe reader syntax. Application-specific objects need an application codec.

`serialize-domain-event` and `deserialize-domain-event` cover event envelopes;
the snapshot and durable-record helpers use the same serializer boundary.

## Recoverable file store

`make-file-event-store` creates a restartable append-log store. It delegates
protocol validation and idempotency to the reference in-memory store and
records operations in a line-oriented journal. `recover-file-event-store`
replays committed records, tolerates an incomplete final record, and signals
`durable-store-corruption` for malformed complete records.

Offset, outbox, and checkpoint files use temporary-file replacement. Their
`:sync` hooks default to `finish-output`: this flushes the Lisp stream but is
not an operating-system `fsync` guarantee. The file lock registry is
process-local, so multi-process or distributed deployment requires a backend
backend with shared locking and transactions.

## Subscriptions

Subscriptions consume the global feed with a durable consumer offset. The
in-memory and file offset stores reject backwards movement. Delivery is
at-least-once: an application acknowledges a delivery only after its work has
completed, and a restart may deliver an unacknowledged batch again.

`make-subscription` requires a globally positioned event store, an offset
store, a consumer ID, and a positive batch size. Filters are application
functions; scheduling and process supervision remain outside the runtime.

## Outbox

The outbox API provides message records, pending reads, leases, claim tokens,
acknowledgement, retry, requeue, and dead-letter state. File-backed messages
are persisted with replacement writes. `event-store-append-with-outbox`
expresses the event-plus-message boundary; the reference combined store
supports it in memory, while a production backend should implement both
writes in its own database transaction.

The file outbox reader requires every field in the current delivery-state
record. It does not migrate older wire records; migrate those files before
opening them with this runtime.

The outbox is an integration primitive, not a message broker. It does not
provide distributed delivery, external publication, or a global scheduler.

## Restartable projections and evolution

Projection checkpoint records persist opaque state, global position, and
update metadata. `make-durable-projection-runner` combines a projection,
event store, and checkpoint store; `run-projection-once` performs one
synchronous run and persists the result after successful handling.

The upcaster registry supports exact schema-version transitions. The observed
store and retry helpers provide operational hooks without changing event-store
semantics. Retention APIs expose capability and floor information; pruning
policy remains a backend and application decision.

## Explicit limits

The reference runtime does not provide replication, encryption,
authorization, metrics export, distributed scheduling, background
supervision, or operating-system `fsync` guarantees. Treat those as explicit
backend or application responsibilities.
