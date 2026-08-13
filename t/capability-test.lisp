(in-package #:cl-event-sourcing-kit/test)

(describe
 "event-store capability discovery"
 (it-each
  ((:append)
   (:read)
   (:read-all)
   (:optimistic-concurrency)
   (:idempotent-event-ids)
   (:batch-append)
   (:global-position)
   (:snapshots)
   (:retention))
  "advertises the in-memory ~A guarantee"
  (capability)
  (expect
   (event-store-supports-p (make-event-store) capability)
   :to-be-truthy))
 (it
  "does not imply durability for the in-memory reference"
  (let ((store (make-event-store)))
    (expect (event-store-supports-p store :durable) :to-be nil)
    (expect (event-store-supports-p store :crash-recovery) :to-be nil)
    (expect (event-store-capabilities store)
            :to-equal
            (remove-duplicates
             (event-store-capabilities store)
             :test #'eq))))
 (it
  "provides neutral capability defaults for an store"
  (let ((store (make-instance 'unsupported-store)))
    (expect (event-store-capabilities store) :to-be nil)
    (expect (event-store-supports-p store :append) :to-be nil)))
 (it
  "reports the guarantees and limits of the durable file reference"
  (call-with-durable-test-path
   "capabilities"
   (lambda (path)
     (let ((store (make-file-event-store :path path)))
       (unwind-protect
            (progn
              (expect (event-store-supports-p store :durable) :to-be-truthy)
              (expect (event-store-supports-p store :crash-recovery)
                      :to-be-truthy)
              (expect (event-store-supports-p store :process-local-lock)
                      :to-be-truthy)
              (expect (event-store-supports-p store :batch-append)
                      :to-be-truthy)
              (expect (event-store-supports-p store :atomic-outbox) :to-be nil))
         (close-file-event-store store))))))
 (it
  "adds atomic outbox semantics only to the composite in-memory store"
  (let ((store (make-in-memory-event-outbox-store)))
    (expect (member :atomic-outbox
                    (event-store-capabilities store)
                    :test #'eq)
            :to-be-truthy)
    (expect (event-store-supports-p store :atomic-outbox) :to-be-truthy)
    (expect (event-store-supports-p store :global-position) :to-be-truthy)
    (expect (event-store-supports-p store :durable) :to-be nil)))
 (it
  "preserves delegate capabilities through observation"
  (let* ((store (make-event-store))
         (observed (make-observed-event-store store)))
    (expect (event-store-capabilities observed)
            :to-equal
            (event-store-capabilities store))))
 (it
  "rejects non-keyword capability names"
  (signals type-error
    (event-store-supports-p (make-event-store) 1)))
 (it
  "requires a store to advertise every requested capability"
  (let ((store (make-event-store)))
    (expect (event-store-require-capabilities
             store
             '(:append :read :append))
            :to-be
            store)
    (expect (event-store-require-capabilities store nil)
            :to-be
            store)))
 (it
  "reports all missing capabilities before an operation starts"
  (let ((condition nil)
        (store (make-event-store)))
    (handler-case
        (event-store-require-capabilities
         store
         '(:durable :replication :durable))
      (event-store-operation-not-supported (caught)
        (setf condition caught)))
    (expect condition :to-be-truthy)
    (expect (event-store-operation condition)
            :to-equal
            '(:required-capabilities (:durable :replication)))
    (expect (event-store-operation-store condition)
            :to-be
            store)))
 (it
  "rejects malformed capability requirement lists"
  (signals type-error
    (event-store-require-capabilities (make-event-store) '(:append . :read)))
  (signals type-error
    (event-store-require-capabilities (make-event-store) '(:append 1))))
 (it
  "covers append-request default keys"
  (signals invalid-domain-event
    (make-event-append-request))
  (let ((request (make-event-append-request :stream-id "defaults")))
    (expect (event-append-request-expected-version request)
            :to-be
            :any))))
