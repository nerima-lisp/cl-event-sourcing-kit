# Getting started

## Load the system

The core is an ASDF system. Add the optional system that matches the code you
need:

```lisp
(asdf:load-system "cl-event-sourcing-kit")
(asdf:load-system "cl-event-sourcing-kit/in-memory")
```

The in-memory system is useful for examples, tests, and applications that
want a small reference adapter. It does not change the core API.

## Append and replay

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
                (cl-event-sourcing-kit:domain-event-payload
                 current-event)))))))
```

The result is `(1 42)`. The reducer is independent of the storage adapter, so
the same aggregate code can be used with a different implementation of
`event-store`.

## Add the durable system

```lisp
(asdf:load-system "cl-event-sourcing-kit/durable")
```

The durable system depends on the in-memory and projection systems and adds a
portable file reference implementation. See [Durable runtime](guide/durable-runtime.md)
for its guarantees and limits before using it as a deployment boundary.

## Dependencies

| Use | ASDF dependency |
| --- | --- |
| Core | `cl-boundary-kit` |
| In-memory and durable systems | `cl-concurrent-kit` in addition to the core |
| Repository tests and coverage | `cl-host-kit` and `cl-weave` |

The repository's Nix flake pins the development inputs. Run `nix develop` for
the reproducible shell; the commands are collected in
[Development](project/development.md).

## First design decisions

Before writing an adapter, decide:

1. What transaction or atomic-write mechanism protects an append batch.
2. How opaque payloads and metadata are serialized.
3. How event IDs are made unique and how equivalent retries are recognized.
4. Whether global positions, snapshots, retention, and recovery are
   supported.
5. Which guarantees are supplied by the backend and which are only adapter
   policy.

The core protocol deliberately does not infer these properties from a class
name.
