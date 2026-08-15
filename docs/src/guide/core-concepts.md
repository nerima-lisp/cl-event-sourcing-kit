# Core concepts

## Event envelope

`make-domain-event` creates an immutable envelope. Its application-facing
fields are opaque values:

| Field | Meaning |
| --- | --- |
| `id` | Event identity used for idempotency. |
| `type` | Application-defined, non-`NIL` event type. |
| `stream-id` | Stream identity used for optimistic concurrency. |
| `aggregate-id` | Optional alias for the stream identity; if both are supplied, they must agree. |
| `payload` | Application data; the core does not choose a representation. |
| `metadata` | Contextual data such as tracing or actor information. |
| `timestamp` | Application-supplied or injected clock value. |
| `schema-version` | Non-negative schema version, defaulting to `1`. |
| `version` | Store-assigned stream sequence, or `NIL` before commit. |
| `correlation-id` | Optional relationship to a causation chain. |
| `causation-id` | Optional event that caused this event. |
| `global-position` | Optional store-wide position, or `NIL` when unsupported. |

The public readers do not expose writable slots. Payload and metadata remain
opaque references, however: the core does not deep-copy or freeze application
values. Supply immutable values or copy them at the application boundary when
that distinction matters.

## Identity and time

Event IDs and timestamps are injectable. A caller can pass explicit `:id` and
`:timestamp` values, or supply `:id-source` and `:clock` collaborators.
Default collaborators are available through `*default-event-id-source*` and
`*default-event-clock*`. The core does not require UUIDs or a particular time
representation.

This keeps tests deterministic and leaves identity policy with the
application. A persistent backend must still enforce the uniqueness and
equivalence policy described in [Event store](event-store.md).

## Stream versions

Committed stream versions start at `1`. A missing stream has current version
`0` and does not exist according to `event-store-stream-exists-p`. An event
created for staging has no assigned version or global position; those values
are added by a successful append.

## Responsibility boundary

The core guarantees protocol-level validation and result shapes. A backend or
application owns:

- transaction and locking behavior;
- serialization and deserialization;
- durability and recovery;
- replication, authorization, and retention;
- scheduling and process supervision.

A backend's `event-store` implementation is therefore a protocol implementation,
not a promise that the backing system is durable or distributed.
