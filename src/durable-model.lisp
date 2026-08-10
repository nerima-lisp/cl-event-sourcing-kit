(in-package #:cl-event-sourcing-kit)

(defclass event-serializer ()
  ((encode :initarg :encode :initform nil :reader %event-serializer-encode)
   (decode :initarg :decode :initform nil :reader %event-serializer-decode)
   (max-bytes
    :initarg :max-bytes
    :initform 1048576
    :reader %event-serializer-max-bytes)
   (max-depth
    :initarg :max-depth
    :initform 64
    :reader %event-serializer-max-depth)))

(defclass file-event-store (event-store)
  ((path :initarg :path :reader file-event-store-path)
   (serializer :initarg :serializer :reader %file-event-store-serializer)
   (delegate :initarg :delegate :accessor %file-event-store-delegate)
   (stream :initarg :stream :accessor %file-event-store-stream)
   (sync :initarg :sync :reader %file-event-store-sync)
   (lock :initarg :lock :reader %file-event-store-lock)
   (global-position-start
    :initarg :global-position-start
    :reader %file-event-store-global-position-start)
   (next-transaction-id
    :initarg :next-transaction-id
    :accessor %file-event-store-next-transaction-id)))

(defclass subscription-offset-store ()
  ())

(defclass in-memory-subscription-offset-store (subscription-offset-store)
  ((offsets :initarg :offsets :reader %in-memory-offsets)
   (lock :initarg :lock :reader %in-memory-offset-lock)))

(defclass file-subscription-offset-store (subscription-offset-store)
  ((path :initarg :path :reader %file-offset-path)
   (serializer :initarg :serializer :reader %file-offset-serializer)
   (sync :initarg :sync :reader %file-offset-sync)
   (lock :initarg :lock :reader %file-offset-lock)))

(defgeneric subscription-offset (store consumer-id))
(defgeneric save-subscription-offset (store consumer-id global-position))

;; A lease is deliberately separate from an offset.  An offset records
;; progress; a lease records the current owner and its fencing token.  A
;; database or coordination-service adapter can therefore implement the
;; same protocol with a compare-and-set/transactional primitive.
(defstruct (subscription-lease
            (:constructor %make-subscription-lease
                (consumer-id owner-id fencing-token expires-at))
            (:copier nil))
  consumer-id
  owner-id
  fencing-token
  expires-at)

(defclass subscription-lease-store
  ()
  ())

(defclass in-memory-subscription-lease-store (subscription-lease-store)
  ((leases :initarg :leases :reader %in-memory-subscription-leases)
   (next-tokens :initarg :next-tokens
                :reader %in-memory-subscription-lease-next-tokens)
   (lock :initarg :lock :reader %in-memory-subscription-lease-lock)))

(defgeneric subscription-lease-acquire
    (store consumer-id owner-id &key now lease-seconds))
(defgeneric subscription-lease-renew
    (store lease &key now lease-seconds))
(defgeneric subscription-lease-release (store lease))
(defgeneric subscription-lease-valid-p (store lease &key now))
(defgeneric subscription-lease-store-capabilities (store))

(defstruct (subscription-delivery
            (:constructor %make-subscription-delivery
                (events after-global-position))
            (:copier nil))
  events
  after-global-position)

(defclass subscription ()
  ((event-store :initarg :event-store :reader %subscription-event-store)
   (offset-store :initarg :offset-store :reader %subscription-offset-store)
   (consumer-id :initarg :consumer-id :reader subscription-consumer-id)
   (cursor :initarg :cursor :accessor subscription-cursor)
   (batch-size :initarg :batch-size :reader %subscription-batch-size)
   (filter :initarg :filter :reader %subscription-filter)
   (lock :initarg :lock :reader %subscription-lock)
   (in-flight :initarg :in-flight :accessor %subscription-in-flight)
   (lease-store :initarg :lease-store :initform nil
                :reader %subscription-lease-store)
   (owner-id :initarg :owner-id :initform nil :reader subscription-owner-id)
   (lease-seconds :initarg :lease-seconds :initform 60
                  :reader subscription-lease-seconds)
   (lease :initarg :lease :initform nil :accessor %subscription-lease)))

(defgeneric subscription-poll (subscription))
(defgeneric subscription-ack (subscription global-position))
(defgeneric deliver-subscription (subscription handler))

(defstruct (outbox-message
            (:constructor %make-outbox-message
                (id topic payload metadata created-at status attempts claimed-at
                    available-at claim-token last-error dead-lettered-at))
            (:copier nil))
  id
  topic
  payload
  metadata
  created-at
  status
  attempts
  claimed-at
  available-at
  claim-token
  last-error
  dead-lettered-at)

(defclass outbox-store ()
  ())

(defclass in-memory-outbox-store (outbox-store)
  ((messages :initarg :messages :reader %in-memory-outbox-messages)
   (ordered-messages :initarg :ordered-messages
                     :reader %in-memory-outbox-ordered-messages)
   (message-index :initarg :message-index :reader %in-memory-outbox-index)
   (lock :initarg :lock :reader %in-memory-outbox-lock)))

(defclass file-outbox-store (outbox-store)
  ((path :initarg :path :reader file-outbox-store-path)
   (serializer :initarg :serializer :reader %file-outbox-serializer)
   (sync :initarg :sync :reader %file-outbox-sync)
   (lock :initarg :lock :reader %file-outbox-lock)))

(defgeneric outbox-append (store message))
(defgeneric outbox-read (store message-id))
(defgeneric outbox-read-all (store &key status limit))
(defgeneric outbox-read-pending (store &key now limit))
(defgeneric outbox-claim (store &key now limit lease-seconds))
(defgeneric outbox-ack (store message-id &key claim-token))
(defgeneric outbox-fail (store message-id
                         &key now backoff-seconds claim-token failure max-attempts))
(defgeneric outbox-requeue (store message-id &key available-at))
(defgeneric outbox-dispatch (store handler
                             &key now limit lease-seconds backoff-seconds
                             max-attempts))

(defclass event-outbox-store ()
  ((event-store :initarg :event-store :reader %event-outbox-event-store)
   (outbox-store :initarg :outbox-store :reader %event-outbox-outbox-store)
   (lock :initarg :lock :reader %event-outbox-lock)))

(defgeneric event-store-append-with-outbox
    (store stream-id events messages &key expected-version))

(defstruct (projection-checkpoint-record
            (:constructor %make-projection-checkpoint-record
                (state position updated-at))
            (:copier nil))
  state
  position
  updated-at)

(defclass projection-checkpoint-store ()
  ())

(defclass in-memory-projection-checkpoint-store (projection-checkpoint-store)
  ((checkpoints :initarg :checkpoints :reader %in-memory-checkpoints)
   (lock :initarg :lock :reader %in-memory-checkpoint-lock)))

(defclass file-projection-checkpoint-store (projection-checkpoint-store)
  ((path :initarg :path :reader %file-checkpoint-path)
   (serializer :initarg :serializer :reader %file-checkpoint-serializer)
   (sync :initarg :sync :reader %file-checkpoint-sync)
   (lock :initarg :lock :reader %file-checkpoint-lock)))

(defgeneric projection-checkpoint-load (store key))
(defgeneric projection-checkpoint-save (store key record))
(defgeneric projection-checkpoint-delete (store key))

(defclass durable-projection-runner ()
  ((projection :initarg :projection :reader durable-projection-runner-projection)
   (event-store :initarg :event-store :reader durable-projection-runner-event-store)
   (checkpoint-store
    :initarg :checkpoint-store
    :reader durable-projection-runner-checkpoint-store)
   (checkpoint-key :initarg :checkpoint-key :reader %durable-runner-key)
   (lock :initarg :lock :reader %durable-runner-lock)))

(defgeneric run-projection-once (runner &key limit))

(defclass upcaster-registry ()
  ((table :initarg :table :reader %upcaster-table)))

(defgeneric register-upcaster
    (registry event-type from-version to-version function))
(defgeneric upcaster-registry-function (registry &key event-type))

(defclass observed-event-store (event-store)
  ((delegate :initarg :delegate :reader %observed-event-store-delegate)
   (before :initarg :before :reader %observed-event-store-before)
   (after :initarg :after :reader %observed-event-store-after)
   (on-error :initarg :on-error :reader %observed-event-store-on-error)))
