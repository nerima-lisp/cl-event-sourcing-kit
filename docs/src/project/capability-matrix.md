# Capability matrix

Event sourcing has two different meanings of "production ready": a library
can provide precise stream semantics, while a deployment still needs a
storage, coordination, security, and operations policy. This matrix records
both sides so that a backend cannot accidentally inherit a guarantee from the
portable reference runtime.

## Capabilities

| Concern | Reference runtime | Guarantee and boundary |
| --- | --- | --- |
| Event envelope | Core protocol | Opaque payload and metadata, event ID, type, stream ID, timestamp, schema, correlation, and causation values are preserved and validated at the protocol boundary. |
| Stream consistency | In-memory and file store | `:no-stream`, `:any`, and integer expected versions provide optimistic per-stream concurrency checks. A database backend must implement the same check in its transaction. |
| Idempotent append | In-memory and file store | Retrying the same event ID with an equivalent envelope is accepted; reusing an ID for a different event is a conflict. |
| Atomic batch append | In-memory and journaled file store | A batch is validated before mutation and is applied as one reference-store operation. A production backend must map this to one database transaction. |
| Global ordering | In-memory and file store | Global positions support bounded `read-all` and durable subscription cursors within one local store. A distributed feed or broker is outside the library. |
| Snapshots | In-memory and journaled file store | Snapshot version and stream identity are checked; file snapshot changes are journaled and recovered. Snapshot cadence and compaction remain application policy. |
| Retention | In-memory and journaled file store | Pruning and retention-floor capability are explicit protocol operations. Deletion policy, legal hold, archival, and cross-replica coordination belong to the backend. |
| Aggregate replay | Core protocol | Replay is pure and payload-agnostic; aggregate loading can combine snapshots, event reads, and an upcaster function. |
| Schema evolution | Durable system | Exact schema-version transitions, envelope-preserving upcasts, observed storage, and bounded retry wrappers are available. Long-lived migration policy remains application-owned. |
| Staging and commands | Core protocol and macros | Staged events, expected version, and CPS commit/rebuild entry points are explicit. Command validation and authorization remain application responsibilities. |
| Projections | Projection and durable systems | Synchronous projection, rebuild, bounded catch-up, checkpoint persistence, restart, and rollback of the reference in-memory state with a supplied state copier are provided. Scheduling, supervision, and read-model database transactions are external. |
| Subscriptions | Durable system | Monotonic offsets, filtering, poll/ack, at-least-once redelivery, local leases, renewal, and fencing tokens are provided. Cross-process consumer groups require a shared coordination backend. |
| Outbox | Durable system | Pending/in-flight/delivered/dead-letter states, claim tokens, retry, requeue, and backoff are provided. Event-plus-outbox atomicity is implemented by the reference in-memory composite; a production backend must use its own transaction. |
| Recovery | Durable file store | Prepare/commit/abort journaling, replay of an incomplete transaction, malformed-record detection, and replacement-file recovery are provided. The default stream flush is not an OS `fsync`. |
| Serialization | Durable system | The default codec is a bounded safe S-expression subset, and custom codecs are injectable. JSON, MessagePack, compatibility migrations, encryption, key rotation, and redaction require an application codec or backend. |
| Concurrency | Reference runtime | In-memory operations use the configured local lock and file operations are process-local. Database locks, leases shared by hosts, replication, and failover are not implied. |
| Operational hooks | Durable system | Observation callbacks, retry wrappers, capability negotiation, corruption conditions, and explicit result/condition values expose operational boundaries. Metrics export, tracing transport, alerting, and supervision remain external. |
| CQRS | Projection primitive | Projections can form read models, but command routing, read consistency, authorization, and query API design are not prescribed. |
| Saga/workflow | Event metadata only | Correlation and causation IDs are available for integration; orchestration, compensation, timers, and workflow durability are not implemented. |
| Audit/compliance | Event history primitive | Immutable event IDs and metadata support an audit design, but retention holds, redaction, access control, legal export, and audit presentation require a separate policy. |
| Deployment | Library boundary | The package is portable Common Lisp reference code. It is not a distributed database, message broker, scheduler, high-availability service, or background supervisor. |

## Backend acceptance checklist

A production backend is complete only when it documents and tests each item
below for its own deployment target:

- expected-version behavior, duplicate event IDs, event equivalence, and
  atomic batch failure;
- transaction isolation, stream ordering, global-position allocation, and
  crash recovery after each durable write boundary;
- snapshot consistency, retention floor, archival, and migration behavior;
- serialization limits, schema evolution, encryption, key rotation, and
  compatibility with older records;
- subscription offset atomicity, lease ownership, fencing, retry, and
  redelivery after process or host failure;
- event-plus-outbox atomicity, claim ownership, external publication, and
  dead-letter operations;
- projection checkpoint atomicity, rebuild semantics, backpressure, and
  supervision;
- authorization, audit policy, observability, backup/restore, replication,
  failover, and the exact durability/fsync contract.

The protocol's `event-store-capabilities` and the durable component capability
functions are the machine-readable starting point for making those claims
explicit. A capability must be advertised only when the backend's tests prove
the corresponding guarantee.
