# cl-event-sourcing-kit

`cl-event-sourcing-kit` is a domain-independent Common Lisp event-sourcing
protocol. It defines an opaque event envelope, optimistic stream appends,
pure replay, staging, structured conditions, and synchronous continuation
entry points without choosing a database, serialization format, or domain
model.

[Documentation](https://nerima-lisp.github.io/cl-event-sourcing-kit/) ·
[Documentation source](docs/src/index.md) ·
[Capability matrix](docs/src/project/capability-matrix.md) ·
[Changelog](CHANGELOG.md)

## Quick start

Load the optional in-memory system for a small public-API-only example:

```lisp
(asdf:load-system "cl-event-sourcing-kit/in-memory")

(let* ((store (cl-event-sourcing-kit:make-event-store))
       (event (cl-event-sourcing-kit:make-domain-event
               :id "quick-start-1"
               :type :value-added
               :stream-id "quick-stream"
               :payload 42
               :timestamp 1)))
  (multiple-value-bind (events version)
      (cl-event-sourcing-kit:event-store-append
       store "quick-stream" (list event) :expected-version :no-stream)
    (list version
          (cl-event-sourcing-kit:replay-events
           0 events
           (lambda (state current-event)
             (+ state
                (cl-event-sourcing-kit:domain-event-payload current-event)))))))
```

The result is `(1 42)`. Replace the in-memory store with an adapter that
implements the same event-store protocol when persistence is required.

## Systems

- `cl-event-sourcing-kit`: storage-independent core protocol, envelope,
  replay, staging, conditions, and CPS entry points.
- `cl-event-sourcing-kit/in-memory`: reference in-memory event store.
- `cl-event-sourcing-kit/projection`: projection and rebuild support.
- `cl-event-sourcing-kit/durable`: safe serialization, recoverable file
  storage, subscriptions, outbox delivery, durable projection checkpoints,
  upcasters, retention, and operational wrappers.

The durable system is a portable reference runtime, not a distributed
database, broker, scheduler, or high-availability deployment. Future database
adapters and separate CQRS, Saga, audit-log, and messaging systems remain
outside this repository's core.

## Development

Run the tests from a configured Common Lisp environment:

```sh
env CL_SOURCE_REGISTRY="$PWD//" sbcl --non-interactive \
  --eval '(require :asdf)' \
  --eval '(asdf:test-system "cl-event-sourcing-kit")'
```

The reproducible Nix commands are:

```sh
nix develop
nix flake check
nix build .#docs
```

See the [development guide](docs/src/project/development.md) for test,
coverage, benchmark, and documentation commands.

## Repository layout

- `docs/`: MkDocs Material site configuration and source pages.
- `src/`: core, projection, in-memory, and durable implementation files.
- `t/`: contract-oriented and public API tests.
- `cl-event-sourcing-kit.asd`: system definitions and dependencies.
- `flake.nix`: reproducible development shell, packages, and checks.

## License

MIT. See [`LICENSE`](LICENSE).
