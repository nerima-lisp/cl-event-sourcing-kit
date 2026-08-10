# Event store

The event store protocol is expressed as CLOS generic functions on
`event-store`. The smallest useful adapter implements append, stream reads,
version inspection, stream existence, and the capability methods relevant to
its storage model.

## Expected versions

`event-store-append` accepts `:expected-version` as an optimistic concurrency
condition:

| Expected version | Missing stream | Existing stream |
| --- | --- | --- |
| `:any` | Succeeds. | Succeeds at any current version. |
| `:no-stream` | Succeeds. | Signals `event-version-conflict`. |
| Non-negative integer `N` | Succeeds only when `N` is `0`. | Succeeds only when the current version is `N`. |

An invalid value signals `invalid-expected-version`. A mismatch signals
`event-version-conflict`, which carries the stream ID, expected version, and
actual version.

An empty append does not create a stream. Events retain input order, and a
successful append returns the committed event envelopes plus the new stream
version. The returned envelopes carry their assigned stream versions and,
when supported, global positions.

## Reads and capabilities

`event-store-read` returns a stream in append order and supports inclusive
`:from-version` and `:to-version` bounds. `event-store-read-all` is the global
feed and is meaningful only when
`event-store-global-position-supported-p` is true. The same capability style
is used for snapshots and retention.

The base methods signal `event-store-operation-not-supported` with a
structured operation value. An adapter should advertise unsupported
capabilities rather than silently returning a weaker result.

## Atomic batches

`make-event-append-request` describes one stream append. A call to
`event-store-append-batch` accepts a list or vector of requests and returns
`event-append-result` values in request order.

The batch is one consistency boundary: every request, event ID, stream
version, and global-position assignment must succeed or none of the streams,
the event-ID index, or the global feed may change. A backend that cannot
provide this boundary may signal `event-store-operation-not-supported`; it
must not present several independent writes as an atomic batch.

## Event ID idempotency

Event IDs are unique across the store. The reference in-memory semantics are:

1. Re-appending an equivalent ID returns the canonical committed event without
   advancing the stream version.
2. Equivalence compares the envelope fields while ignoring store-assigned
   `version` and `global-position`. An adapter may specialize
   `event-store-event-equivalent-p`.
3. Reusing an ID for a different envelope signals
   `duplicate-event-id-conflict`.
4. Repeating an ID inside one request, or mixing an idempotent duplicate with
   new events in one request, signals `duplicate-event-id`.

A duplicate-only retry remains safe even when its expected version is stale.
A request containing new events still performs its normal expected-version
check.

## Snapshots

`event-snapshot` captures opaque aggregate state after a stream version.
Snapshot support is optional and is advertised by
`event-store-snapshots-supported-p`.

The reference semantics require a snapshot not to be ahead of the current
stream, retain historical versions, and replace a snapshot saved at the same
version. `event-store-read-snapshot` selects the newest snapshot at or before
a requested version; `event-store-delete-snapshot` removes one stream's
snapshot history.

`load-aggregate` uses the newest available snapshot by default, then replays
only events after that version. It validates snapshot and event versions and
requires the loaded stream to be contiguous. Snapshot state, metadata,
serialization, and consistency with the event log remain adapter concerns.

## Adapter checklist

Document these choices in every persistent adapter:

- transaction or atomic-write mechanism for append batches;
- serialization format and treatment of opaque values;
- concurrent version and event-ID allocation;
- recovery, replication, retention, and durability guarantees;
- global-feed ordering and checkpoint behavior;
- snapshot consistency and pruning policy.

These decisions belong in an adapter repository such as a future PostgreSQL or
Redis integration, not in the storage-independent core.
