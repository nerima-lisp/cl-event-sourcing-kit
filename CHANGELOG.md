# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [1.0.0]

Initial stable release.

### Added

- Storage-independent event-store protocol: opaque event envelopes,
  optimistic stream append/read semantics, pure replay, staging, structured
  conditions, and synchronous continuation (CPS) entry points.
- `cl-event-sourcing-kit/in-memory`: thread-safe reference in-memory event
  store with snapshots, global positions, batch append, and idempotent event
  ids.
- `cl-event-sourcing-kit/projection`: projection state and rebuild support,
  including restartable, checkpointed rebuilds over the global feed.
- `cl-event-sourcing-kit/durable`: safe S-expression serialization,
  crash-tolerant file-backed event log, at-least-once subscriptions with
  leasing and fencing, application outbox with retry and dead-letter
  handling, durable projection checkpoints, and schema upcasting.
- Capability discovery protocol so adapters can advertise and callers can
  require specific store guarantees before an operation runs.
- Documentation site (MkDocs Material) covering getting started, guides,
  capability matrix, scope, and roadmap.
- Reproducible Nix flake with `default` (test), `coverage`,
  `coverage-strict`, `docs`, and `formatting` checks; 100% expression and
  branch coverage on the executable operation and recovery modules.
- Benchmark script (`bench/performance.lisp`) for the in-memory store and
  outbox.

[1.0.0]: https://github.com/nerima-lisp/cl-event-sourcing-kit/releases/tag/v1.0.0
