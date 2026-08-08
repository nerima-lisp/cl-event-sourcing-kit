(in-package #:cl-event-sourcing-kit)

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
     (format
      stream
      "~A (reason ~S, event ~S)."
      (event-sourcing-error-message condition)
      (invalid-domain-event-reason condition)
      (invalid-domain-event-event condition)))))

(define-condition invalid-expected-version (event-sourcing-error)
  ((value :initarg :value :reader invalid-expected-version-value))
  (:default-initargs
   :message
   "Expected version must be :ANY, :NO-STREAM, or a non-negative integer.")
  (:report
   (lambda (condition stream)
     (format
      stream
      "~A (value ~S)."
      (event-sourcing-error-message condition)
      (invalid-expected-version-value condition)))))

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
     (format
      stream
      "~A (stream ~S, expected ~S, actual ~S)."
      (event-sourcing-error-message condition)
      (event-version-conflict-stream-id condition)
      (event-version-conflict-expected-version condition)
      (event-version-conflict-actual-version condition)))))

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
   "The event store operation is not supported by this adapter.")
  (:report
   (lambda (condition stream)
     (format
      stream
      "~A (operation ~S)."
      (event-sourcing-error-message condition)
      (event-store-operation condition)))))

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
