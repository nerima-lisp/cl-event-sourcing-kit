# cl-event-sourcing-kit

`cl-event-sourcing-kit` is a domain-independent Common Lisp event-sourcing
protocol. It provides the consistency boundary around an event stream without
choosing a database, serialization format, business model, or messaging
system. The optional `cl-event-sourcing-kit/in-memory` system supplies the
reference store used by the examples and tests.

The event payload and metadata are opaque Lisp values. The core does not know
whether they contain a plist, a structure, a JSON-compatible tree, a byte
vector, or an application-defined object.

## Quick start

The core system is storage-independent. Load the optional in-memory system for
a small, public-API-only example:

```lisp
(asdf:load-system "cl-event-sourcing-kit/in-memory")

(let* ((store (cl-event-sourcing-kit:make-event-store))
       (event (cl-event-sourcing-kit:make-domain-event
               :id "quick-start-1"
               :type :value-added
               :stream-id "quick-stream"
               :payload 42
               :metadata '(:source :example)
               :timestamp 1)))
  (multiple-value-bind (committed-events version)
      (cl-event-sourcing-kit:event-store-append
       store "quick-stream" (list event) :expected-version :no-stream)
    (list version
          (cl-event-sourcing-kit:replay-events
           0 committed-events
           (lambda (state current-event)
             (+ state
                (cl-event-sourcing-kit:domain-event-payload current-event)))))))
```

The result is `(1 42)`. An application can replace the in-memory store with
an adapter implementing the same protocol without changing its aggregate
replay function.

## Domain event envelope

`make-domain-event` creates a read-only envelope with these generic fields:

- `id`: the event identity used for idempotency.
- `type`: an application-defined, non-`NIL` event type.
- `stream-id`: the stream identity used for optimistic concurrency.
- `aggregate-id`: an optional alias for `stream-id`; the two values must not
  disagree.
- `payload`: opaque application data.
- `metadata`: opaque contextual data.
- `timestamp`: an opaque timestamp value.
- `version`: the stream sequence assigned by a store, or `NIL` before commit.
- `correlation-id` and `causation-id`: optional extension points for tracing
  and causal relationships.
- `global-position`: an optional store-wide position, or `NIL` when the
  adapter does not provide one.

Event IDs and timestamps are injectable. Supply `:id` and `:timestamp` for
explicit values, or use `:id-source` and `:clock` for per-event collaborators.
Those collaborators may be functions or boundary-kit source objects. Tests
can also rebind `*default-event-id-source*` and `*default-event-clock*`. The
core does not require UUIDs or a particular clock representation.

Envelope slots are not writable through the public API. Payload and metadata
remain opaque references, however: the core does not deep-copy or freeze
application values. Applications that need value immutability should supply
immutable values or copy them at their own boundary.

## Event store protocol

The protocol is expressed as CLOS generic functions on `event-store`.
Persistent implementations should subclass `event-store` directly and
implement the operations needed by their storage model:

- `event-store-append`
- `event-store-read`
- `event-store-read-all` when global positions are available
- `event-store-current-version`
- `event-store-current-global-position` when global positions are available
- `event-store-stream-exists-p`
- `event-store-global-position-supported-p`
- `event-store-event-equivalent-p` when duplicate comparison needs adapter
  policy

The base methods signal `event-store-operation-not-supported` with a
structured `event-store-operation` value. An adapter owns its transaction
boundary, locking, retry behavior, serialization, recovery, and durability;
the core does not infer any of those properties from the adapter class.

## Append semantics

### Expected versions

Stream versions start at `1`. A stream that has never had a successful append
has current version `0` and `event-store-stream-exists-p` returns `NIL`. An
empty append does not create a stream.

| `:expected-version` | Missing stream | Existing stream |
| --- | --- | --- |
| `:any` | succeeds | succeeds at any current version |
| `:no-stream` | succeeds | signals `event-version-conflict` |
| non-negative integer `N` | succeeds only when `N` is `0` | succeeds only when current version is `N` |

An integer mismatch signals `event-version-conflict`, which exposes the
stream ID, expected version, and actual version. Invalid expected-version
values signal `invalid-expected-version`.

### Atomicity and ordering

An append batch is validated before the in-memory store mutates any stream,
event index, or global feed. A failed expected-version check, invalid event,
duplicate conflict, or assigned-position error leaves the batch unapplied.
Adapters must provide the same all-or-nothing protocol boundary using their
native transaction mechanism; the core cannot make a non-transactional
adapter durable or atomic by itself.

Events retain input order in a stream. A successful append returns two values:
the committed event envelopes and the new stream version. The returned
envelopes are new envelopes carrying their assigned stream versions. The
in-memory implementation also assigns monotonically increasing global
positions in append order across streams.

`event-store-read` returns stream events in append order and supports inclusive
`from-version` and `to-version` bounds. Global positions are optional. The
in-memory store supports them; an adapter that does not can report `NIL` from
`event-store-global-position-supported-p` and signal an unsupported operation
for global-feed reads.

### Event ID idempotency

Event IDs are unique across the in-memory store, not only within one stream.
The following behavior is intentional:

1. Re-appending an equivalent event ID is idempotent. The store returns the
   canonical committed event and does not advance the stream version.
2. Equivalence compares the envelope's event fields and ignores store-assigned
   `version` and `global-position`. Adapters may specialize
   `event-store-event-equivalent-p`.
3. Reusing an event ID for a different envelope signals
   `duplicate-event-id-conflict`.
4. Repeating an ID inside one request, or combining an idempotent duplicate
   with new events in one request, signals `duplicate-event-id`.

The duplicate-only retry is safe to repeat even when the retry's expected
version is stale, because the store has already recorded that event ID. A
request that contains new events still performs the normal expected-version
check.

## Replay and staging

`replay-events` is a pure left-to-right fold. It does not sort, serialize,
persist, or mutate the event list, so an aggregate can be represented by any
state value and reducer function:

```lisp
(cl-event-sourcing-kit:replay-events
 initial-state events
 (lambda (state event)
   (reduce-state state
                 (cl-event-sourcing-kit:domain-event-payload event))))
```

`make-event-staging`, `stage-event`, `uncommitted-events`, and `commit-events`
provide a small mutable session for one stream. Staged events are uncommitted
and therefore have no version or global position. A successful commit clears
the staging list and advances its expected version. A conflict or other
append failure leaves the staged events available for inspection or retry.

## Projection and rebuild

The optional `cl-event-sourcing-kit/projection` system provides
`make-projection` and `rebuild-projection`. A projection handler receives the
current state and one event and returns the next state. Rebuild starts from an
initial state and consumes the adapter's global feed after a checkpoint.

The checkpoint advances only after a handler returns successfully. If a
handler signals an error, `projection-failure` exposes the projection, failed
event, global position, original cause, and last successful checkpoint. The
state and checkpoint are updated independently only after a successful handler
return. The core does not roll back side effects performed by a handler on a
mutable object; immutable state or application-owned transactional projection
storage is recommended.

The core deliberately does not provide a projection daemon, scheduler,
background worker, retry loop, or checkpoint database. Those belong to an
application or a separate operational system.

## What this is, and what it is not

- **Event sourcing** stores the events as the source of aggregate state and
  reconstructs state by replay. This library defines that stream protocol and
  a reference store.
- **CQRS** separates command/write models from query/read models. A projection
  can be used by a CQRS application, but CQRS routing and read-model policy
  are outside this core.
- **Saga** coordinates a long-running business workflow and compensating
  actions. Correlation and causation IDs are extension points only; this
  library does not implement orchestration or compensation.
- **Audit log** records a history for review or compliance. An audit log may
  use an event store, but audit retention, redaction, authorization, and
  presentation are separate concerns.
- **Outbox pattern** atomically records a message alongside a local business
  transaction and publishes it later. This event store is not an outbox and
  does not publish messages.

Keeping these boundaries explicit prevents a database adapter or event
envelope from silently becoming a message broker, workflow engine, or query
model framework.

## Systems and future adapters

- `cl-event-sourcing-kit`: the storage-independent core protocol, pure replay,
  staging, structured conditions, and synchronous continuation entry points.
- `cl-event-sourcing-kit/in-memory`: the named in-memory implementation system
  for applications that want to express that dependency explicitly.
- `cl-event-sourcing-kit/projection`: optional projection/rebuild support.
- `cl-host-kit`: host portability for the test and coverage bootstrap; it is
  not part of the core event-store protocol.
- Future `cl-event-sourcing-postgresql-kit`: PostgreSQL storage, transactions,
  serialization, and durability policy.
- Future `cl-event-sourcing-redis-kit`: Redis storage and its availability/
  durability trade-offs.
- Future separate CQRS, Saga, audit-log, and outbox integration systems:
  `cl-event-sourcing-cqrs-kit`, `cl-event-sourcing-saga-kit`,
  `cl-audit-log-kit`, and `cl-event-sourcing-outbox-kit` are intentionally
  outside this repository's core.

Neither PostgreSQL, Redis, filesystem storage, `cl-postgresql-kit`, nor
`cl-redis-kit` is a dependency of this system. No JSON, MessagePack, or S-expression
serialization is selected by the core.

## Testing

Run the ASDF test operation from the repository checkout:

```sh
env CL_SOURCE_REGISTRY="$PWD//" sbcl --non-interactive \
  --eval '(require :asdf)' \
  --eval '(asdf:test-system "cl-event-sourcing-kit")'
```

The repository wrapper runs the same test entry point and is suitable for CI:

```sh
env CL_SOURCE_REGISTRY="$PWD//" sbcl --script run-tests.lisp
```

The flake provides the same reproducible environment:

```sh
nix develop -c sbcl --non-interactive \
  --eval '(require :asdf)' \
  --eval '(asdf:test-system "cl-event-sourcing-kit")'
```

Run the strict expression/branch coverage gate with:

```sh
env CL_SOURCE_REGISTRY="$PWD//" sbcl --non-interactive \
  --load run-coverage.lisp
```

The flake exposes the same checks as `nix flake check`. Build the regular
coverage report with `nix build .#coverage`; the strict expression/branch gate
is the `coverage-strict` check included in `nix flake check`.

The coverage runner fails when the instrumented core operations do not reach
100% expression and branch coverage. Coverage output is written to the
directory selected by `CL_EVENT_SOURCING_KIT_COVERAGE_DIR`, or a temporary
directory when that variable is absent.

The test system covers envelope construction and injected collaborators,
stream reads and writes, all expected-version cases, conflicts, duplicate ID
semantics, ordering, pure replay, staging, projection rebuild and failure
checkpoints, opaque values, the adapter protocol, the in-memory reference
store, and a public-API-only quick start.
