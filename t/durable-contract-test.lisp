(in-package #:cl-event-sourcing-kit/test)

(describe
 "durable model contracts"
 (it
  "constructs serializers and rejects invalid serializer options"
  (let ((serializer
          (make-event-serializer
           :encode #'princ-to-string
           :decode #'read-from-string
           :max-bytes 128
           :max-depth 8)))
    (expect (event-serializer-p serializer) :to-be-truthy)
    (expect (event-serializer-p nil) :to-be nil)
    (expect (serialize-value :ready :serializer serializer) :to-equal "READY")
    (expect (deserialize-value ":READY" :serializer serializer) :to-be :ready))
  (signals error (make-event-serializer :encode 1))
  (signals error (make-event-serializer :decode 1))
  (signals error (make-event-serializer :max-bytes 0))
  (signals error
    (make-event-serializer :max-bytes "not-an-integer"))
  (signals error (make-event-serializer :max-depth 0))
  (signals error
    (make-event-serializer :max-depth "not-an-integer")))
 (it
  "constructs outbox messages and validates their state"
  (let ((message
          (make-outbox-message
           :id "message-1"
           :topic "orders"
           :payload '(:order 1)
           :metadata '(:source :test)
           :created-at 100
           :status :dead-letter
           :attempts 2
           :claimed-at 101
           :available-at 102
           :claim-token "claim-1"
           :last-error "failure"
           :dead-lettered-at 103)))
    (expect (outbox-message-p message) :to-be-truthy)
    (expect (outbox-message-id message) :to-equal "message-1")
    (expect (outbox-message-status message) :to-be :dead-letter)
    (expect (outbox-message-attempts message) :to-be 2)
    (expect (outbox-message-last-error message) :to-equal "failure"))
  (signals error (make-outbox-message :topic "orders"))
  (signals error (make-outbox-message :id "message-1"))
  (signals error (make-outbox-message :id "message-1" :topic "orders" :attempts -1))
  (signals error
    (make-outbox-message
     :id "message-1"
     :topic "orders"
     :attempts "not-an-integer"))
  (signals error
    (make-outbox-message :id "message-1" :topic "orders" :status :unknown))
  (signals error
    (make-outbox-message :id "message-1" :topic "orders" :last-error 1))
  (signals error
    (make-outbox-message
     :id "message-1" :topic "orders" :dead-lettered-at "not-a-time")))
 (it
  "constructs checkpoints and observed stores"
  (let* ((record
           (make-projection-checkpoint-record
            :state '(:total 4)
            :position 4
            :updated-at 200))
         (store (make-in-memory-event-store))
         (observed
           (make-observed-event-store
            store
            :before (lambda (operation) (declare (ignore operation)))
            :after (lambda (operation result)
                     (declare (ignore operation result)))
            :on-error (lambda (operation condition)
                        (declare (ignore operation condition))))))
    (expect (projection-checkpoint-record-p record) :to-be-truthy)
    (expect (projection-checkpoint-record-state record) :to-equal '(:total 4))
    (expect (projection-checkpoint-record-position record) :to-be 4)
    (expect (observed-event-store-p observed) :to-be-truthy)
    (expect (observed-event-store-p nil) :to-be nil))
  (signals error (make-projection-checkpoint-record :position -1))
  (signals error (make-observed-event-store nil))
  (signals error
    (make-observed-event-store
     (make-in-memory-event-store)
     :before 1))
  (signals error
    (make-observed-event-store
     (make-in-memory-event-store)
     :after 1))
  (signals error
    (make-observed-event-store
     (make-in-memory-event-store)
     :on-error 1)))
 (it
  "constructs durable store predicates"
  (let ((offsets (make-in-memory-subscription-offset-store))
        (checkpoints (make-in-memory-projection-checkpoint-store))
        (outbox (make-in-memory-outbox-store)))
    (expect (subscription-offset-store-p offsets) :to-be-truthy)
    (expect (projection-checkpoint-store-p checkpoints) :to-be-truthy)
    (expect (outbox-store-p outbox) :to-be-truthy)
    (expect (subscription-offset-store-p nil) :to-be nil)
    (expect (projection-checkpoint-store-p nil) :to-be nil)
    (expect (outbox-store-p nil) :to-be nil))))

(describe
 "durable serialization contracts"
 (it
  "round-trips custom values and enforces codec boundaries"
  (let ((serializer
          (make-event-serializer
           :encode (lambda (value) (format nil "value:~A" value))
           :decode (lambda (text)
                     (subseq text (length "value:")))
           :max-bytes 32)))
    (expect (serialize-value 42 :serializer serializer) :to-equal "value:42")
    (expect (deserialize-value "value:42" :serializer serializer)
            :to-equal "42")
    (signals event-serialization-error
      (serialize-value 42 :serializer
                       (make-event-serializer :encode (lambda (value)
                                                       (declare (ignore value))
                                                       42))))
    (signals event-serialization-error
      (deserialize-value "value:42" :serializer
                         (make-event-serializer :decode (lambda (text)
                                                           (declare (ignore text))
                                                           (error "decode")))))
    (signals event-serialization-error
      (serialize-value "123456789" :serializer
                       (make-event-serializer
                        :encode #'princ-to-string
                        :max-bytes 2)))
    (signals event-serialization-error
      (deserialize-value "123456789" :serializer
                         (make-event-serializer :max-bytes 2)))))
 (it
  "rejects unsafe and malformed default wire values"
  (let ((event (make-test-event "wire-event" "wire-stream" '(:ok t)))
        (snapshot
          (make-event-snapshot
           :stream-id "wire-stream"
           :version 1
           :state '(:balance 10)
           :metadata nil
           :timestamp 100)))
    (expect (deserialize-value (serialize-value '(:ok #(:a :b))))
            :to-equalp
            '(:ok #(:a :b)))
    (expect (domain-event-id (deserialize-domain-event
                              (serialize-domain-event event)))
            :to-equal
            "wire-event")
    (expect (event-snapshot-state
             (cl-event-sourcing-kit::%snapshot-from-wire
              (cl-event-sourcing-kit::%snapshot-wire snapshot)))
            :to-equal
            '(:balance 10))
    (signals event-serialization-error (serialize-value (lambda () t)))
    (signals event-serialization-error (deserialize-value ""))
    (signals event-serialization-error (deserialize-value "1 2"))
    (signals event-serialization-error (deserialize-value "#.(+ 1 2)"))
    (signals event-serialization-error (deserialize-value "#P\"/tmp/x\""))
    (signals error
      (cl-event-sourcing-kit::%domain-event-from-wire '(:domain-event :id "only-id")))
    (signals error
      (cl-event-sourcing-kit::%domain-event-from-wire '(:not-an-event)))
    (signals error
      (cl-event-sourcing-kit::%snapshot-from-wire '(:not-a-snapshot)))
    (signals error
      (cl-event-sourcing-kit::%snapshot-from-wire '(:event-snapshot :stream-id "s")))
  (let ((cycle (list :cycle)))
    (setf (cdr cycle) cycle)
    (signals event-serialization-error (serialize-value cycle)))
  (signals error
    (cl-event-sourcing-kit::%safe-serializable-value-p :ok :max-depth 0))
  (expect
   (cl-event-sourcing-kit::%safe-serializable-value-p (make-symbol "PRIVATE"))
   :to-be nil)
  (expect
   (cl-event-sourcing-kit::%safe-serializable-value-p '(1 (2 3)) :max-depth 8)
   :to-be-truthy)
  (expect
   (cl-event-sourcing-kit::%safe-serializable-value-p '(1 (2 3)) :max-depth 1)
   :to-be nil)
  (expect (cl-event-sourcing-kit::%unsafe-reader-dispatch-p "#.")
          :to-be-truthy)
  (expect (cl-event-sourcing-kit::%unsafe-reader-dispatch-p "plain")
          :to-be nil))))

(describe
 "durable offsets and checkpoints"
 (it
  "persists offsets and checkpoints through file stores"
  (call-with-durable-test-path
   "contract-offsets"
   (lambda (path)
     (let ((offsets (make-file-subscription-offset-store :path path)))
       (expect (subscription-offset offsets "consumer") :to-be 0)
       (expect (subscription-offset offsets "consumer") :to-be 0)
        (save-subscription-offset offsets "other" 3)
        (expect (subscription-offset offsets "other") :to-be 3)
        (signals error
          (save-subscription-offset offsets "bad" -1))
        (signals error
          (save-subscription-offset offsets "bad" "not-a-number"))
       (signals error (subscription-offset offsets nil))
        (expect (subscription-offset offsets "") :to-be 0)))))
 (it
  "saves and deletes memory and file checkpoints"
  (let ((memory (make-in-memory-projection-checkpoint-store))
        (record (make-projection-checkpoint-record :state 1 :position 1)))
    (expect (projection-checkpoint-load memory "missing") :to-be nil)
    (projection-checkpoint-save memory "sum" record)
    (expect (projection-checkpoint-record-position
             (projection-checkpoint-load memory "sum"))
            :to-be 1)
    (signals error
      (projection-checkpoint-save
       memory "sum" (make-projection-checkpoint-record :state 2 :position 0)))
    (expect (projection-checkpoint-record-position
             (projection-checkpoint-load memory "sum"))
            :to-be 1)
    (signals error (projection-checkpoint-load memory nil))
    (signals error (projection-checkpoint-save memory nil record))
    (projection-checkpoint-delete memory "sum")
    (expect (projection-checkpoint-load memory "sum") :to-be nil))
  (call-with-durable-test-path
   "contract-checkpoints"
   (lambda (path)
     (let* ((store (make-file-projection-checkpoint-store :path path))
            (record (make-projection-checkpoint-record
                     :state '(:total 2) :position 2 :updated-at 300)))
       (expect (projection-checkpoint-load store "missing") :to-be nil)
       (projection-checkpoint-save store "sum" record)
       (expect (projection-checkpoint-record-state
                (projection-checkpoint-load store "sum"))
               :to-equal '(:total 2))
       (projection-checkpoint-delete store "sum")
       (expect (projection-checkpoint-load store "sum") :to-be nil)
       (signals error (projection-checkpoint-load store nil))
       (signals error (projection-checkpoint-save store nil record)))))))
