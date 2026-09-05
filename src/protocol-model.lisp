(in-package #:cl-event-sourcing-kit)

(defclass event-snapshot ()
  ((stream-id :initarg :stream-id :reader event-snapshot-stream-id)
   (version :initarg :version :reader event-snapshot-version)
   (state :initarg :state :reader event-snapshot-state)
   (metadata :initarg :metadata :reader event-snapshot-metadata)
   (timestamp :initarg :timestamp :reader event-snapshot-timestamp))
  (:documentation
   "An opaque aggregate state captured after a stream version."))

(defstruct (event-append-request
            (:constructor %make-event-append-request
                (stream-id events expected-version))
            (:copier nil))
  stream-id
  events
  expected-version)

(defstruct (event-append-result
            (:constructor %make-event-append-result (events version))
            (:copier nil))
  events
  version)

(defmacro define-store-operation (name lambda-list &optional documentation)
  "Declare one store operation from its data-level protocol specification."
  `(defgeneric ,name ,lambda-list
     ,@(when documentation `((:documentation ,documentation)))))

(define-store-operation event-store-append
    (store stream-id events &key expected-version))

(define-store-operation event-store-read
    (store stream-id &key from-version to-version))

(define-store-operation event-store-read-all
    (store &key after-global-position limit))

(define-store-operation event-store-current-version (store stream-id))

(define-store-operation event-store-current-global-position (store))

(define-store-operation event-store-stream-exists-p (store stream-id))

(define-store-operation event-store-global-position-supported-p (store))

(define-store-operation event-store-event-equivalent-p
    (store existing-event requested-event))

(define-store-operation event-store-append-batch (store requests))

(define-store-operation event-store-save-snapshot (store snapshot))

(define-store-operation event-store-read-snapshot
    (store stream-id &key version))

(define-store-operation event-store-delete-snapshot (store stream-id))

(define-store-operation event-store-snapshots-supported-p (store))

(define-store-operation event-store-prune
    (store &key before-global-position))

(define-store-operation event-store-retention-supported-p (store))

(define-store-operation event-store-retention-floor (store))

(define-store-operation event-store-capabilities
    (store)
    "Return the stable capability keywords provided by STORE.

Capability discovery is deliberately additive: backend implementations may expose more
keywords, while callers should only require the capabilities that their
operation needs.  Standard keywords include :APPEND, :READ, :READ-ALL,
:OPTIMISTIC-CONCURRENCY, :IDEMPOTENT-EVENT-IDS, :BATCH-APPEND,
:GLOBAL-POSITION, :SNAPSHOTS, :RETENTION, :DURABLE, :CRASH-RECOVERY,
:PROCESS-LOCAL-LOCK, :ATOMIC-OUTBOX, :CROSS-PROCESS-LOCK,
:CONSUMER-LEASES, :FENCING, :FSYNC, :REPLICATION, and :ENCRYPTION.  The
extended keywords are backend-defined deployment guarantees; the core only
uses them when a caller explicitly requires them.")

(define-store-operation event-store-supports-p
    (store capability)
    "Return true when STORE advertises CAPABILITY.")
