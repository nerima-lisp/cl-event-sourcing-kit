# Recipes

## Append a new stream

Use `:no-stream` when the command is creating a stream:

```lisp
(event-store-append
 store
 "order-1"
 (list event)
 :expected-version :no-stream)
```

For a stream whose current version was read as `7`, pass `:expected-version
7` to detect a concurrent writer.

## Load and update an aggregate

```lisp
(multiple-value-bind (state version)
    (load-aggregate store "order-1" initial-state #'reduce-order)
  (declare (ignore state))
  (event-store-append
   store "order-1" new-events :expected-version version))
```

The default loader uses the newest supported snapshot. Supply
`:use-snapshot NIL` or an `:upcaster` when a command needs a different replay
policy.

## Catch up a projection in bounded batches

```lisp
(make-projection
 :name :order-count
 :initial-state 0
 :handler (lambda (count event)
            (if (eq (domain-event-type event) :order-created)
                (1+ count)
                count)))

(rebuild-projection projection store :limit 100)
(advance-projection projection store :limit 100)
```

The checkpoint is advanced only after each handler call succeeds. Persist it
through the durable runner when a restartable reference runtime is suitable.

## Observe an backend

Wrap a store with `make-observed-event-store` to attach before/after/error
callbacks around protocol operations. This is an observation boundary, not a
replacement for metrics, tracing, or transaction instrumentation supplied by
the deployment.
