# Architecture

The project separates protocol semantics from infrastructure:

```text
application aggregate / command
              |
       replay + staging
              |
       event-store protocol
        /       |       \
   in-memory  file     future database backend
        |       |       \
       snapshots, global feed, retention
              |
        projection / subscription / outbox
```

## Core layer

The core owns the immutable event envelope, expected-version validation,
stream ordering rules, result values, structured conditions, pure replay,
staging, and synchronous CPS entry points. Payloads, metadata, and aggregate
state remain opaque.

## Backend layer

Each backend implements the store-operation protocol directly. The protocol is
defined by generic operations and explicit capability values; there is no
base-store class or compatibility layer. A backend decides how
transactions, locking, serialization, recovery, durability, global positions,
snapshots, and retention work.

The in-memory backend is the executable reference for protocol semantics. The
file backend composes a journal with safe serialization to demonstrate a
restartable local runtime.

## Application layer

Aggregates, command validation, authorization, projection scheduling,
external message publication, supervision, and deployment are application or
integration concerns. A projection or outbox API is a boundary primitive; it
does not turn the library into a CQRS framework, broker, or workflow engine.

## Source layout

| Directory/file | Role |
| --- | --- |
| `src/` | Core, optional systems, and reference implementations. |
| `t/` | Contract-oriented tests and public-API checks. |
| `cl-event-sourcing-kit.asd` | ASDF system composition. |
| `flake.nix` | Reproducible development, tests, coverage, and docs outputs. |
| `docs/` | This MkDocs site, with pages in `docs/src/`. |
