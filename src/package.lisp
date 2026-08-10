(defpackage #:cl-event-sourcing-kit
  (:use #:cl)
  (:documentation
   "Domain-independent event sourcing protocol.

Event payloads and metadata are opaque caller-owned values.  Adapters own
serialization, transactions, durability, and recovery; this package exposes
the protocol and its reference semantics.")
  (:export
   ;; Immutable domain event envelope.
   #:domain-event
   #:domain-event-p
   #:make-domain-event
   #:domain-event-id
   #:domain-event-type
   #:domain-event-stream-id
   #:domain-event-aggregate-id
   #:domain-event-payload
   #:domain-event-metadata
   #:domain-event-timestamp
   #:domain-event-schema-version
   #:domain-event-version
   #:domain-event-correlation-id
   #:domain-event-causation-id
   #:domain-event-global-position
   #:*default-event-id-source*
   #:*default-event-clock*

   ;; Structured conditions.
   #:event-sourcing-error
   #:event-sourcing-error-message
   #:invalid-domain-event
   #:invalid-domain-event-event
   #:invalid-domain-event-reason
   #:invalid-expected-version
   #:invalid-expected-version-value
   #:event-version-conflict
   #:event-version-conflict-stream-id
   #:event-version-conflict-expected-version
   #:event-version-conflict-actual-version
   #:duplicate-event-id
   #:duplicate-event-id-event-id
   #:duplicate-event-id-existing-event
   #:duplicate-event-id-requested-event
   #:duplicate-event-id-conflict
   #:event-store-operation-not-supported
   #:event-store-operation
   #:event-store-operation-store
   #:event-store-retention-gap
   #:event-store-retention-gap-store
   #:event-store-retention-gap-requested-position
   #:event-store-retention-gap-floor
   #:invalid-snapshot
   #:invalid-snapshot-stream-id
   #:invalid-snapshot-version
   #:invalid-snapshot-reason
   #:projection-read-failure
   #:projection-read-failure-projection
   #:projection-read-failure-cause
   #:projection-read-failure-after-global-position
   #:projection-read-failure-checkpoint
   #:projection-failure
   #:projection-failure-projection
   #:projection-failure-event
   #:projection-failure-global-position
   #:projection-failure-cause
   #:projection-failure-checkpoint

   ;; Event store protocol.
   #:event-store
   #:event-store-append
   #:event-store-read
   #:event-store-read-all
   #:event-store-current-version
   #:event-store-current-global-position
   #:event-store-stream-exists-p
   #:event-store-global-position-supported-p
   #:event-store-event-equivalent-p
   #:event-store-append-batch
   #:event-store-save-snapshot
   #:event-store-read-snapshot
   #:event-store-delete-snapshot
   #:event-store-snapshots-supported-p

   ;; Batch append and snapshot values.
   #:event-append-request
   #:event-append-request-p
   #:make-event-append-request
   #:event-append-request-stream-id
   #:event-append-request-events
   #:event-append-request-expected-version
   #:event-append-result
   #:event-append-result-p
   #:event-append-result-events
   #:event-append-result-version
   #:event-snapshot
   #:event-snapshot-p
   #:make-event-snapshot
   #:event-snapshot-stream-id
   #:event-snapshot-version
   #:event-snapshot-state
   #:event-snapshot-metadata
   #:event-snapshot-timestamp

   ;; Schema evolution and aggregate loading.
   #:upcast-event
   #:upcast-events
   #:load-aggregate

   ;; Synchronous continuation entry points.
   #:event-store-append/cc
   #:event-store-append-batch/cc
   #:event-store-read/cc
   #:replay-events/cc
   #:commit-events/cc
   #:rebuild-projection/cc
   #:advance-projection/cc

   ;; Pure replay and staging.
   #:replay-events
   #:event-staging
   #:event-staging-p
   #:make-event-staging
   #:event-staging-stream-id
   #:event-staging-expected-version
   #:stage-event
   #:uncommitted-events
   #:commit-events

   ;; Static DSL helpers.
   #:define-event-reducer
   #:with-event-staging

   ;; Optional projection system.
   #:projection
   #:projection-p
   #:make-projection
   #:projection-name
   #:projection-state
   #:projection-checkpoint
   #:projection-handler
   #:rebuild-projection
   #:advance-projection
   #:define-projection

   ;; Optional in-memory reference system.
   #:in-memory-event-store
   #:in-memory-event-store-p
   #:make-in-memory-event-store
   #:make-event-store

   ;; Optional durable adapter and application-runtime facilities.
   #:event-serialization-error
   #:event-serialization-error-value
   #:event-serialization-error-cause
   #:event-serialization-error-direction
   #:durable-store-corruption
   #:durable-store-corruption-path
   #:durable-store-corruption-record
   #:durable-store-corruption-cause
   #:event-serializer
   #:event-serializer-p
   #:make-event-serializer
   #:serialize-value
   #:deserialize-value
   #:serialize-domain-event
   #:deserialize-domain-event
   #:file-event-store
   #:file-event-store-p
   #:make-file-event-store
   #:file-event-store-path
   #:file-event-store-sync
   #:recover-file-event-store
   #:close-file-event-store

   ;; At-least-once global-feed subscriptions and durable offsets.
   #:subscription-offset-store
   #:subscription-offset-store-p
   #:in-memory-subscription-offset-store
   #:in-memory-subscription-offset-store-p
   #:make-in-memory-subscription-offset-store
   #:file-subscription-offset-store
   #:file-subscription-offset-store-p
   #:make-file-subscription-offset-store
   #:subscription-offset
   #:save-subscription-offset
   #:subscription-lease
   #:subscription-lease-p
   #:make-subscription-lease
   #:subscription-lease-consumer-id
   #:subscription-lease-owner-id
   #:subscription-lease-fencing-token
   #:subscription-lease-expires-at
   #:subscription-lease-store
   #:subscription-lease-store-p
   #:in-memory-subscription-lease-store
   #:in-memory-subscription-lease-store-p
   #:make-in-memory-subscription-lease-store
   #:subscription-lease-acquire
   #:subscription-lease-renew
   #:subscription-lease-release
   #:subscription-lease-valid-p
   #:subscription-lease-store-capabilities
   #:subscription-delivery
   #:subscription-delivery-p
   #:subscription-delivery-events
   #:subscription-delivery-after-global-position
   #:subscription
   #:subscription-p
   #:make-subscription
   #:subscription-consumer-id
   #:subscription-owner-id
   #:subscription-lease-seconds
   #:subscription-current-lease
   #:subscription-renew-lease
   #:subscription-release-lease
   #:subscription-cursor
   #:subscription-poll
   #:subscription-ack
   #:deliver-subscription

   ;; Transactional application outbox protocol and reference implementation.
   #:outbox-message
   #:outbox-message-p
   #:make-outbox-message
   #:outbox-message-id
   #:outbox-message-topic
   #:outbox-message-payload
   #:outbox-message-metadata
   #:outbox-message-created-at
   #:outbox-message-status
   #:outbox-message-attempts
   #:outbox-message-claimed-at
   #:outbox-message-available-at
   #:outbox-message-claim-token
   #:outbox-message-last-error
   #:outbox-message-dead-lettered-at
   #:outbox-message-conflict
   #:outbox-message-conflict-message-id
   #:outbox-message-conflict-existing-message
   #:outbox-message-conflict-requested-message
   #:outbox-store
   #:outbox-store-p
   #:in-memory-outbox-store
   #:in-memory-outbox-store-p
   #:make-in-memory-outbox-store
   #:file-outbox-store
   #:file-outbox-store-p
   #:make-file-outbox-store
   #:outbox-append
   #:outbox-read
   #:outbox-read-all
   #:outbox-read-pending
   #:outbox-claim
   #:outbox-ack
   #:outbox-fail
   #:outbox-requeue
   #:outbox-dispatch
   #:event-outbox-store
   #:event-outbox-store-p
   #:make-in-memory-event-outbox-store
   #:event-store-append-with-outbox

   ;; Restartable projection state/checkpoint persistence.
   #:projection-checkpoint-record
   #:projection-checkpoint-record-p
   #:make-projection-checkpoint-record
   #:projection-checkpoint-record-state
   #:projection-checkpoint-record-position
   #:projection-checkpoint-record-updated-at
   #:projection-checkpoint-store
   #:projection-checkpoint-store-p
   #:in-memory-projection-checkpoint-store
   #:in-memory-projection-checkpoint-store-p
   #:make-in-memory-projection-checkpoint-store
   #:file-projection-checkpoint-store
   #:file-projection-checkpoint-store-p
   #:make-file-projection-checkpoint-store
   #:projection-checkpoint-load
   #:projection-checkpoint-save
   #:projection-checkpoint-delete
   #:durable-projection-runner
   #:durable-projection-runner-p
   #:make-durable-projection-runner
   #:durable-projection-runner-projection
   #:durable-projection-runner-event-store
   #:durable-projection-runner-checkpoint-store
   #:run-projection-once

   ;; Versioned upcaster registry and operational helpers.
   #:upcaster-registry
   #:upcaster-registry-p
   #:make-upcaster-registry
   #:register-upcaster
   #:upcaster-registry-function
   #:observed-event-store
   #:observed-event-store-p
   #:make-observed-event-store
   #:with-retries
   #:event-store-prune
   #:event-store-retention-supported-p
   #:event-store-retention-floor
   #:event-store-capabilities
   #:event-store-supports-p
   #:event-store-require-capabilities))
