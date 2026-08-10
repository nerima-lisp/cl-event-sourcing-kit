# Projections

The optional `cl-event-sourcing-kit/projection` system provides synchronous
global-feed projection operations:

- `make-projection` creates state, a handler, and a non-negative checkpoint;
- `rebuild-projection` resets the state and folds events after a cursor;
- `advance-projection` continues from the current checkpoint;
- `projection-state` and `projection-checkpoint` expose the current result.

The handler receives the current state and one committed event and returns the
next state. Use `:limit` for a bounded batch, then call
`advance-projection` again to continue.

## Checkpoint ordering

The checkpoint advances only after the handler returns successfully. A
handler failure leaves the last successful state and checkpoint visible and
signals `projection-failure` with the projection, event, global position,
cause, and checkpoint.

A failure while reading the global feed signals `projection-read-failure` and
includes the attempted cursor and last successful checkpoint. Invalid events
and non-monotonic global positions are also reported through structured
projection failures.

The core cannot roll back side effects a handler has already performed on an
externally mutable object. Prefer immutable state or an application-owned
transaction around side effects and checkpoint persistence.

## Operational boundary

The projection system is not a daemon or scheduler. Scheduling, process
supervision, retry policy, and deployment belong to the application. The
durable system adds a synchronous, restartable runner and checkpoint stores;
it still does not create a background worker.
