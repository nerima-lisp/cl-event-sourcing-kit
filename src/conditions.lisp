;;;; Package selection is a reader-time declaration.  Keeping it out of the
;;;; compiled form lets SB-COVER measure the condition report functions below
;;;; without counting an uncallable load-time form as runtime behavior.
#.(progn (in-package #:cl-event-sourcing-kit) nil)

(define-condition event-sourcing-error (error)
  ((message
    :initarg
    :message
    :initform
    "Event sourcing operation failed."
    :reader
    event-sourcing-error-message))
  (:report
   (lambda (condition stream)
     (write-string (event-sourcing-error-message condition) stream))))

(define-condition invalid-domain-event (event-sourcing-error)
  ((event :initarg :event :reader invalid-domain-event-event)
   (reason :initarg :reason :reader invalid-domain-event-reason))
  (:default-initargs :message "The domain event is invalid.")
  (:report
   (lambda (condition stream)
     (let ((*print-circle* t))
       (format
        stream
        "~A (reason ~S, event ~S)."
        (event-sourcing-error-message condition)
        (invalid-domain-event-reason condition)
        (invalid-domain-event-event condition))))))

(define-condition invalid-expected-version (event-sourcing-error)
  ((value :initarg :value :reader invalid-expected-version-value))
  (:default-initargs
   :message
   "Expected version must be :ANY, :NO-STREAM, or a non-negative integer.")
  (:report
   (lambda (condition stream)
     (let ((*print-circle* t))
       (format
        stream
        "~A (value ~S)."
        (event-sourcing-error-message condition)
        (invalid-expected-version-value condition))))))

(define-condition event-version-conflict (event-sourcing-error)
  ((stream-id :initarg :stream-id :reader event-version-conflict-stream-id)
   (expected-version
    :initarg
    :expected-version
    :reader
    event-version-conflict-expected-version)
   (actual-version
    :initarg
    :actual-version
    :reader
    event-version-conflict-actual-version))
  (:default-initargs
   :message
   "The stream version does not match the expected version.")
  (:report
   (lambda (condition stream)
     (let ((*print-circle* t))
       (format
        stream
        "~A (stream ~S, expected ~S, actual ~S)."
        (event-sourcing-error-message condition)
        (event-version-conflict-stream-id condition)
        (event-version-conflict-expected-version condition)
        (event-version-conflict-actual-version condition))))))

(define-condition duplicate-event-id (event-sourcing-error)
  ((event-id :initarg :event-id :reader duplicate-event-id-event-id)
   (existing-event
    :initarg
    :existing-event
    :reader
    duplicate-event-id-existing-event)
   (requested-event
    :initarg
    :requested-event
    :reader
    duplicate-event-id-requested-event))
  (:default-initargs
   :message
   "An event id was used more than once in one append."))

(define-condition duplicate-event-id-conflict (duplicate-event-id)
  ()
  (:default-initargs
   :message
   "An event id is already associated with a different event."))

(define-condition event-store-operation-not-supported (event-sourcing-error)
  ((operation :initarg :operation :reader event-store-operation)
   (store :initarg :store :reader event-store-operation-store))
  (:default-initargs
   :message
        "The event store operation is not supported by this store.")
  (:report
   (lambda (condition stream)
     (let ((*print-circle* t))
       (format
        stream
        "~A (operation ~S)."
        (event-sourcing-error-message condition)
        (event-store-operation condition))))))

(define-condition event-store-retention-gap (event-sourcing-error)
  ((store :initarg :store :reader event-store-retention-gap-store)
   (requested-position
    :initarg
    :requested-position
    :reader
    event-store-retention-gap-requested-position)
   (floor :initarg :floor :reader event-store-retention-gap-floor))
  (:default-initargs
   :message
   "The requested global-feed position has been removed by retention.")
  (:report
   (lambda (condition stream)
     (format
      stream
      "~A (requested position ~S, retention floor ~S)."
      (event-sourcing-error-message condition)
      (event-store-retention-gap-requested-position condition)
      (event-store-retention-gap-floor condition)))))

(define-condition invalid-snapshot (event-sourcing-error)
  ((stream-id :initarg :stream-id :reader invalid-snapshot-stream-id)
   (version :initarg :version :reader invalid-snapshot-version)
   (reason :initarg :reason :reader invalid-snapshot-reason))
  (:default-initargs
   :message
   "The aggregate snapshot is invalid.")
  (:report
   (lambda (condition stream)
     (let ((*print-circle* t))
       (format
        stream
        "~A (stream ~S, version ~S, reason ~S)."
        (event-sourcing-error-message condition)
        (invalid-snapshot-stream-id condition)
        (invalid-snapshot-version condition)
        (invalid-snapshot-reason condition))))))

(define-condition projection-read-failure (event-sourcing-error)
  ((projection :initarg :projection :reader projection-read-failure-projection)
   (cause :initarg :cause :reader projection-read-failure-cause)
   (after-global-position
    :initarg
    :after-global-position
    :reader
    projection-read-failure-after-global-position)
   (checkpoint
    :initarg
    :checkpoint
    :reader
    projection-read-failure-checkpoint))
  (:default-initargs
   :message
   "A projection could not read its event source; the checkpoint was not advanced.")
  (:report
   (lambda (condition stream)
     (let ((*print-circle* t))
       (format
        stream
        "~A (projection ~S, after-global-position ~S, checkpoint ~S, cause ~S)."
        (event-sourcing-error-message condition)
        (projection-read-failure-projection condition)
        (projection-read-failure-after-global-position condition)
        (projection-read-failure-checkpoint condition)
        (projection-read-failure-cause condition))))))

(define-condition projection-failure (event-sourcing-error)
  ((projection :initarg :projection :reader projection-failure-projection)
   (event :initarg :event :reader projection-failure-event)
   (global-position
    :initarg
    :global-position
    :reader
    projection-failure-global-position)
   (cause :initarg :cause :reader projection-failure-cause)
   (checkpoint :initarg :checkpoint :reader projection-failure-checkpoint))
  (:default-initargs
   :message
   "A projection handler failed; the checkpoint was not advanced."))

(define-condition event-serialization-error (event-sourcing-error)
  ((value :initarg :value :reader event-serialization-error-value)
   (direction :initarg :direction :reader event-serialization-error-direction)
   (cause :initarg :cause :initform nil :reader event-serialization-error-cause))
  (:default-initargs
   :message
   "A value could not be serialized or deserialized.")
  (:report
   (lambda (condition stream)
     (let ((*print-circle* t))
       (format stream
               "~A (direction ~S, value ~S, cause ~S)."
               (event-sourcing-error-message condition)
               (event-serialization-error-direction condition)
               (event-serialization-error-value condition)
               (event-serialization-error-cause condition))))))

(define-condition durable-store-corruption (event-sourcing-error)
  ((path :initarg :path :reader durable-store-corruption-path)
   (record :initarg :record :initform nil :reader durable-store-corruption-record)
   (cause :initarg :cause :initform nil :reader durable-store-corruption-cause))
  (:default-initargs
   :message
   "A durable store contains invalid or incomplete data.")
  (:report
   (lambda (condition stream)
     (let ((*print-circle* t))
       (format stream
               "~A (path ~S, record ~S, cause ~S)."
               (event-sourcing-error-message condition)
               (durable-store-corruption-path condition)
               (durable-store-corruption-record condition)
               (durable-store-corruption-cause condition))))))

(define-condition outbox-message-conflict (event-sourcing-error)
  ((message-id
    :initarg
    :message-id
    :reader
    outbox-message-conflict-message-id)
   (existing-message
    :initarg
    :existing-message
    :reader
    outbox-message-conflict-existing-message)
   (requested-message
    :initarg
    :requested-message
    :reader
    outbox-message-conflict-requested-message))
  (:default-initargs
   :message
   "An outbox message id is already associated with a different message."))
