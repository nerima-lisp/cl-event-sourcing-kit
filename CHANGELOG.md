# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project intends to follow [Semantic Versioning](https://semver.org/).

## [1.0.0] - 2026-08-17

### Added

- `cl-event-sourcing-kit`: storage-independent core protocol -- opaque event
  envelope, optimistic append/read semantics, pure replay, staging,
  structured conditions, and synchronous CPS entry points.
- `cl-event-sourcing-kit/in-memory`: thread-safe reference event store with
  optimistic concurrency, idempotent append, snapshots, and global positions.
- `cl-event-sourcing-kit/projection`: synchronous projection, rebuild, and
  bounded catch-up support.
- `cl-event-sourcing-kit/durable`: a portable durable reference runtime --
  crash-tolerant append log with prepare/commit/abort journaling, a safe
  S-expression serializer, at-least-once subscriptions with offsets and
  leases, a transactional application outbox, durable projection
  checkpoints, and a versioned upcaster registry.
- `LICENSE` (MIT), matching the license already declared in
  `cl-event-sourcing-kit.asd`.
- A GitHub Actions workflow running `nix flake check` on push and pull
  request.

### Security

- The default safe-deserialization codec now rejects reader datum-label
  syntax (`#n=` / `#n#`). Without this, a sub-kilobyte crafted payload could
  build a DAG with shared, non-circular substructure that took exponential
  time to validate, defeating the codec's depth and byte-size limits.
  `%safe-serializable-value-p` also now memoizes each node once it has been
  verified, so shared substructure introduced by other means is validated
  once rather than once per reference.
- Durable file reads (`%read-file-string`, the append-log line reader) now
  enforce the configured byte limit while reading, instead of after
  buffering an entire file or line into memory.
- `event-serialization-error` and `durable-store-corruption` now truncate an
  oversized string payload at construction, so a rejected value is not
  retained in full for the life of the condition.
- Durable temporary-file publishing (`%write-serialized-file`, append-log
  replacement) now opens the temporary file exclusively and retries only on
  a `probe-file`-confirmed name collision, instead of silently overwriting
  an existing file at the same generated name.

### Fixed

- `t/projection-test.lisp`: a paren-nesting defect had detached two tests
  ("rejects malformed global feed entries without advancing" and "validates
  projection construction and checkpoint input") from the `describe`
  "projection rebuild" they were meant to belong to, and had left the
  `advance-projection` rejection check outside of any named test. Both are
  now correctly nested and named.
- Documentation: `docs/src/getting-started.md` and `docs/src/reference/api.md`
  listed `cl-weave` as a test-only dependency; it is a core-system runtime
  dependency (`src/cps.lisp`, `src/cps-macros.lisp`), matching
  `cl-event-sourcing-kit.asd` and `docs/src/reference/compatibility.md`.
