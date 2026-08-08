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

   ;; Synchronous continuation entry points.
   #:event-store-append/cc
   #:event-store-read/cc
   #:replay-events/cc
   #:commit-events/cc
   #:rebuild-projection/cc

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
   #:define-projection

   ;; Optional in-memory reference system.
   #:in-memory-event-store
   #:in-memory-event-store-p
   #:make-in-memory-event-store
   #:make-event-store))
