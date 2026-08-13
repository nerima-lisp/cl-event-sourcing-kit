# Scope

See the [world-level capability matrix](capability-matrix.md) for the
implementation guarantees and the backend or application responsibilities
behind them.

## Included

- event envelopes and optimistic stream append/read semantics;
- pure aggregate replay, schema upcasting, and staging;
- optional in-memory storage and snapshots;
- optional synchronous projections and rebuilds;
- optional local durable reference components for files, subscriptions,
  outbox messages, checkpoints, and evolution.

## Related but separate

| Concern | Relationship |
| --- | --- |
| CQRS | Projections can support a read model, but command routing and read-model policy are outside the core. |
| Saga | Correlation and causation IDs are extension points; orchestration and compensation are not implemented. |
| Audit log | An event history may support auditing, but retention, redaction, authorization, and presentation are separate. |
| Outbox | The durable system has a reference outbox boundary; external publication and a production transaction belong to the backend/application. |
| Global feed | The core exposes a read protocol; it is not a broker or scheduler. |

## Explicit non-goals

The core does not provide a database, JSON or MessagePack format, distributed
coordination, replication, encryption, authorization, metrics export,
background supervision, or a workflow engine. These responsibilities should
be explicit in the backend or application that needs them.
