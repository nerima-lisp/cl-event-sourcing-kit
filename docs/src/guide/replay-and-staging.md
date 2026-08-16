# Replay and staging

## Pure replay

`replay-events` is a left-to-right fold:

```lisp
(cl-event-sourcing-kit:replay-events
 initial-state events
 (lambda (state event)
   (reduce-state state
                 (cl-event-sourcing-kit:domain-event-payload event))))
```

The reducer receives the current state and one immutable domain event and
returns the next state. Replay does not sort, persist, serialize, or mutate
the event sequence.

## Schema evolution

`upcast-event` and `upcast-events` are the schema-evolution boundary. An
upcaster receives a stored `domain-event` and returns a domain event in the
shape expected by the current reducer. The upcaster can be a registry, a
chain, or a backend-specific function; the core does not select a wire
format. It may change the payload and schema version, but must preserve the
event envelope: identity, type, stream and aggregate identity, metadata,
timestamp, stream version, correlation and causation identifiers, and global
position. Violating this rule signals `event-sourcing-error`, because changing
those fields would alter ordering or event identity during replay.

`load-aggregate` accepts the same `:upcaster` policy and validates the event
stream before reducing it. Pass `:use-snapshot NIL` when a caller needs a full
replay even if the backend supports snapshots. It returns the reconstructed
state and the last stream version, with `0` for an empty stream.

## Staging a command

`event-staging` is a small mutable session for one stream:

```lisp
(let ((staging
        (cl-event-sourcing-kit:make-event-staging "account-1")))
  (cl-event-sourcing-kit:stage-event
   staging
   (cl-event-sourcing-kit:make-domain-event
    :id "event-1"
    :type :account-opened
    :stream-id "account-1"
    :payload '(:currency :jpy)))
  (cl-event-sourcing-kit:commit-events staging store))
```

The default expected version is `:no-stream`; pass an expected version when
continuing an existing stream. Staged events must belong to the staging
stream and must not already have a stream version or global position.

`uncommitted-events` returns a copy of the pending list. A successful
`commit-events` clears the list and advances the session's expected version.
An append failure leaves the staged events available for inspection or retry.

## Synchronous and CPS entry points

The synchronous operations are the primary API. The optional CPS macros expose
the same boundaries through success and error continuations for applications
that use continuation-passing control flow. The exported entry points are
`event-store-append/cc`, `event-store-append-batch/cc`, `event-store-read/cc`,
`replay-events/cc`, `commit-events/cc`, `rebuild-projection/cc`, and
`advance-projection/cc`. They run synchronously and do not change transaction
semantics.
