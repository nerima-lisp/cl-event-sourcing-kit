(in-package #:cl-event-sourcing-kit/test)

(describe
 "durable boundary validation contracts"
 (it "covers invalid records and default public arguments"
   (flet ((raw-outbox-message (id topic status attempts last-error
                                dead-lettered-at)
            (cl-event-sourcing-kit::%make-outbox-message
             id topic nil nil 0 status attempts nil 0 nil
             last-error dead-lettered-at)))
     (dolist (message
              (list
               (raw-outbox-message nil "topic" :pending 0 nil nil)
               (raw-outbox-message
                cl-event-sourcing-kit::*unspecified*
                "topic"
                :pending
                0
                nil
                nil)
               (raw-outbox-message "id" nil :pending 0 nil nil)
               (raw-outbox-message
                "id"
                cl-event-sourcing-kit::*unspecified*
                :pending
                0
                nil
                nil)
               (raw-outbox-message "id" "topic" :invalid 0 nil nil)
               (raw-outbox-message "id" "topic" :pending -1 nil nil)))
       (signals event-sourcing-error
         (make-in-memory-outbox-store :messages (list message))))
     (signals type-error
       (make-in-memory-outbox-store
        :messages
        (list
         (raw-outbox-message "id" "topic" :pending 0 42 nil))))
     (signals type-error
       (make-in-memory-outbox-store
        :messages
        (list
         (raw-outbox-message "id" "topic" :pending 0 nil "invalid"))))
     (signals type-error
       (make-in-memory-outbox-store :lock 1))
     (signals type-error
       (make-in-memory-event-outbox-store :event-store nil))
     (signals type-error
       (make-in-memory-event-outbox-store :outbox-store nil))
     (signals type-error
       (make-in-memory-event-outbox-store :lock 1))
     (signals event-store-operation-not-supported
       (event-store-append-with-outbox
        (make-instance 'event-store)
        "unsupported"
        nil
        nil))
     (let ((invalid-store
             (make-instance
              'cl-event-sourcing-kit::event-outbox-store
              :event-store nil
              :outbox-store (make-in-memory-outbox-store)
              :lock (cl-event-sourcing-kit::%durable-lock
                     "coverage-invalid-event-outbox"))))
       (signals event-store-operation-not-supported
         (event-store-append-with-outbox
          invalid-store
          "unsupported-composite"
          nil
          nil)))
     (signals event-sourcing-error
       (make-file-outbox-store))
     (let ((serializer (make-event-serializer)))
       (expect (event-serializer-p serializer) :to-be t))
     (let ((message
             (make-outbox-message
              :id "default-message"
              :topic "default-topic"
              :payload nil)))
       (expect (outbox-message-status message) :to-be :pending)
       (expect (outbox-message-attempts message) :to-be 0))
     (let ((store (make-in-memory-outbox-store)))
       (expect (length (outbox-read-all store)) :to-be 0))
     (let ((combined (make-in-memory-event-outbox-store)))
       (multiple-value-bind (events version)
           (event-store-append-with-outbox
            combined
            "default-expected-version"
            (list
             (make-test-event
              "default-expected-event"
              "default-expected-version"
              :payload))
            (list (edge-outbox-message "default-expected-message")))
         (expect (length events) :to-be 1)
         (expect version :to-be 1)))
     (let* ((message-id "memory-default-lifecycle")
            (store
              (make-in-memory-outbox-store
               :messages (list (edge-outbox-message message-id)))))
       (signals event-sourcing-error
         (outbox-ack store message-id :claim-token nil))
       (expect (length (outbox-read-pending store)) :to-be 1)
       (expect (length (outbox-read-all store)) :to-be 1)
       (expect (outbox-message-status (outbox-fail store message-id))
               :to-be
               :pending)
       (expect (outbox-message-status (outbox-requeue store message-id))
               :to-be
               :pending)
       (expect (length (outbox-claim store)) :to-be 1))
     (let ((store
             (make-in-memory-outbox-store
              :messages (list (edge-outbox-message "memory-dispatch")))))
       (multiple-value-bind (delivered claimed dead-lettered)
           (outbox-dispatch
            store
            (lambda (message)
              (declare (ignore message))))
         (expect delivered :to-be 1)
         (expect claimed :to-be 1)
         (expect dead-lettered :to-be 0)))
     (call-with-durable-test-path
      "file-outbox-default-lifecycle"
      (lambda (path)
        (let* ((store (make-file-outbox-store :path path))
               (message-id "file-default-lifecycle"))
          (outbox-append store (edge-outbox-message message-id))
          (signals event-sourcing-error
            (outbox-ack store message-id :claim-token nil))
          (expect (length (outbox-read-pending store)) :to-be 1)
          (expect (length (outbox-read-all store)) :to-be 1)
          (expect (outbox-message-status (outbox-fail store message-id))
                  :to-be
                  :pending)
          (expect (outbox-message-status (outbox-requeue store message-id))
                  :to-be
                  :pending)
          (expect (length (outbox-claim store)) :to-be 1)
          (outbox-append store (edge-outbox-message "file-dispatch"))
          (multiple-value-bind (delivered claimed dead-lettered)
              (outbox-dispatch
               store
               (lambda (message)
                 (declare (ignore message))))
            (expect delivered :to-be 1)
            (expect claimed :to-be 1)
            (expect dead-lettered :to-be 0))))))))

(describe
 "durable coverage guards"
 (it "executes indirect defaults and serializer guard paths"
   (let ((serializer
           (funcall
            (symbol-function 'cl-event-sourcing-kit::make-event-serializer))))
     (expect (event-serializer-p serializer) :to-be t))
   (let ((message
           (funcall
            (symbol-function 'cl-event-sourcing-kit::make-outbox-message)
            :id "indirect-default-message"
            :topic "indirect-default-topic"
            :payload nil)))
     (expect (outbox-message-status message) :to-be :pending)
     (expect (outbox-message-attempts message) :to-be 0))
   (expect (cl-event-sourcing-kit::%safe-serializable-value-p :ok)
           :to-be-truthy)
   (expect
    (cl-event-sourcing-kit::%safe-serializable-value-p
     (vector (vector (vector :too-deep)))
     :max-depth 1)
    :to-be
    nil)
   (signals event-serialization-error
     (deserialize-value
      "already-a-condition"
      :serializer
      (make-event-serializer
       :decode (lambda (text)
                 (error 'event-serialization-error
                        :value text
                        :direction :decode
                        :cause "already wrapped"))))))

 (it "covers projection checkpoint validation and durable recovery guards"
   (signals type-error
     (cl-event-sourcing-kit::%copy-projection-checkpoint-record nil))
   (signals type-error
     (cl-event-sourcing-kit::%validate-projection-checkpoint-record nil))
   (signals event-sourcing-error
     (cl-event-sourcing-kit::%validate-projection-checkpoint-record
      (cl-event-sourcing-kit::%make-projection-checkpoint-record
       :state
       -1
       nil)))
   (signals event-sourcing-error
     (cl-event-sourcing-kit::%validate-projection-checkpoint-record
      (cl-event-sourcing-kit::%make-projection-checkpoint-record
       :state
       "not-an-integer"
       nil)))
   (call-with-durable-test-path
    "projection-coverage"
    (lambda (path)
      (signals event-sourcing-error
        (funcall
         (symbol-function
          'cl-event-sourcing-kit::make-file-projection-checkpoint-store)))
      (signals type-error
        (make-file-projection-checkpoint-store :path path :sync 1))
      (let ((store (make-file-projection-checkpoint-store :path path)))
        (write-durable-edge-value path :wrong-envelope)
        (signals durable-store-corruption
          (projection-checkpoint-load store "wrong-envelope"))
        (write-durable-edge-value
         path
         '(:projection-checkpoints (:wrong-record)))
        (signals durable-store-corruption
          (projection-checkpoint-load store "wrong-record"))
        (signals durable-store-corruption
          (cl-event-sourcing-kit::%projection-checkpoint-table-from-wire
           (list :projection-checkpoints :not-a-record)
           path))
        (write-durable-edge-value
         path
         '(:projection-checkpoints (:projection-checkpoint)))
        (signals durable-store-corruption
          (projection-checkpoint-load store "missing-field"))
        (let ((record
                '(:projection-checkpoint
                  :key "duplicate"
                  :state nil
                  :position 0
                  :updated-at nil)))
          (write-durable-edge-value
           path
           (list :projection-checkpoints record record)))
        (signals durable-store-corruption
          (projection-checkpoint-load store "duplicate"))
        (write-durable-edge-value path nil)
        (expect (projection-checkpoint-delete store "missing") :to-be nil)
        (projection-checkpoint-save
         store
         "backwards"
         (make-projection-checkpoint-record
          :state nil
          :position 2
          :updated-at 2))
        (signals event-sourcing-error
          (projection-checkpoint-save
           store
           "backwards"
           (make-projection-checkpoint-record
            :state nil
            :position 1
            :updated-at 1)))
        (expect (projection-checkpoint-delete store "backwards") :to-be t)
        (let ((failing-store
                (make-file-projection-checkpoint-store
                 :path path
                 :sync (lambda (stream)
                         (declare (ignore stream))
                         (error "projection sync failure")))))
          (signals error
            (projection-checkpoint-save
             failing-store
             "sync-failure"
             (make-projection-checkpoint-record
              :state nil
              :position 0
              :updated-at 0)))))))))
