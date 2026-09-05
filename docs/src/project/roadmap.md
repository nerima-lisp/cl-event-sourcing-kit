# Roadmap

This page records direction and boundaries, not release dates.

## Current reference surface

- storage-independent event-store protocol;
- in-memory semantics for optimistic concurrency, idempotency, snapshots, and
  global positions;
- synchronous replay, staging, and projections;
- local durable reference components with explicit limitations.

## Stability rule

New infrastructure should enter the core only when it is a protocol concern
that remains independent of a backend and deployment model. A feature that
needs a scheduler, broker, distributed lock, or application policy belongs in
an optional system or a separate backend.
