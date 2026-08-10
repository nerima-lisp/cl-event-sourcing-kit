(in-package #:cl-event-sourcing-kit)

(defclass event-store ()
  ()
  (:documentation
   "Abstract event-store protocol.

An adapter subclasses this class and implements the generic operations.  The
   class is intentionally not an adapter wrapper or factory: storage ownership,
   serialization, transactions, durability, and recovery remain with the
   adapter."))

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

(defgeneric event-store-append (store stream-id events &key expected-version))

(defgeneric event-store-read (store stream-id &key from-version to-version))

(defgeneric event-store-read-all (store &key after-global-position limit))

(defgeneric event-store-current-version (store stream-id))

(defgeneric event-store-current-global-position (store))

(defgeneric event-store-stream-exists-p (store stream-id))

(defgeneric event-store-global-position-supported-p (store))

(defgeneric event-store-event-equivalent-p (store
                                            existing-event
                                            requested-event))

(defgeneric event-store-append-batch (store requests))

(defgeneric event-store-save-snapshot (store snapshot))

(defgeneric event-store-read-snapshot (store stream-id &key version))

(defgeneric event-store-delete-snapshot (store stream-id))

(defgeneric event-store-snapshots-supported-p (store))

(defgeneric event-store-prune (store &key before-global-position))

(defgeneric event-store-retention-supported-p (store))

(defgeneric event-store-retention-floor (store))

(defgeneric event-store-capabilities (store)
  (:documentation
   "Return the stable capability keywords provided by STORE.

Capability discovery is deliberately additive: adapters may expose more
keywords, while callers should only require the capabilities that their
operation needs.  Standard keywords include :APPEND, :READ, :READ-ALL,
:OPTIMISTIC-CONCURRENCY, :IDEMPOTENT-EVENT-IDS, :BATCH-APPEND,
:GLOBAL-POSITION, :SNAPSHOTS, :RETENTION, :DURABLE, :CRASH-RECOVERY,
:PROCESS-LOCAL-LOCK, :ATOMIC-OUTBOX, :CROSS-PROCESS-LOCK,
:CONSUMER-LEASES, :FENCING, :FSYNC, :REPLICATION, and :ENCRYPTION.  The
extended keywords are adapter-defined deployment guarantees; the core only
uses them when a caller explicitly requires them."))

(defgeneric event-store-supports-p (store capability)
  (:documentation
   "Return true when STORE advertises CAPABILITY."))
