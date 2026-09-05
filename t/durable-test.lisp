(in-package #:cl-event-sourcing-kit/test)

(defun durable-test-path (suffix)
  (merge-pathnames
   (format nil "cl-event-sourcing-kit-~A-~A" suffix (gensym "TEST-"))
   (host-kit:temporary-directory)))

(defun call-with-durable-test-path (suffix function)
  (let ((path (durable-test-path suffix)))
    (unwind-protect
         (funcall function path)
      (when (probe-file path)
        (delete-file path)))))

(describe
 "durable serialization"
 (it
  "round-trips safe event data and rejects unsafe values"
  (let* ((event
           (make-test-event
            "durable-event-1"
            "durable-stream"
            (list :amount 42 :tags #(:alpha :beta))
            :type :order-created
            :metadata (list :source :test :nested (list 1 2 3))))
         (serialized (serialize-domain-event event))
         (round-trip (deserialize-domain-event serialized)))
    (expect (domain-event-id round-trip) :to-equal "durable-event-1")
    (expect (domain-event-type round-trip) :to-be :order-created)
    (expect (domain-event-stream-id round-trip) :to-equal "durable-stream")
    (expect (domain-event-payload round-trip)
            :to-equalp
            '(:amount 42 :tags #(:alpha :beta)))
    (expect (domain-event-metadata round-trip)
            :to-equal
            '(:source :test :nested (1 2 3)))
    (signals event-serialization-error
      (serialize-value (lambda () t)))
    (let ((cycle (list :cycle)))
      (setf (cdr cycle) cycle)
      (signals event-serialization-error (serialize-value cycle)))))
 (it
  "enforces serializer byte and nesting limits"
  (signals event-serialization-error
    (serialize-value "too-long"
                     :serializer (make-event-serializer :max-bytes 4)))
  (signals event-serialization-error
    (serialize-value (list (list (list :too-deep)))
                     :serializer (make-event-serializer :max-depth 1)))))

(describe
 "durable event log"
 (it
  "recovers events and snapshots after a restart"
  (call-with-durable-test-path
   "event-log"
   (lambda (path)
     (let ((store (make-file-event-store
                   :path path
                   :global-position-start 0)))
       (unwind-protect
            (let ((event (make-test-event
                          "file-event-1"
                          "file-stream"
                          '(:value 7))))
              (multiple-value-bind (committed version)
                  (event-store-append
                   store
                   "file-stream"
                   (list event)
                   :expected-version
                   :no-stream)
                (expect (event-store-event-equivalent-p
                         store
                         event
                         (first committed))
                        :to-be-truthy)
                (expect version :to-be 1)
                (event-store-save-snapshot
                 store
                 (make-event-snapshot
                  :stream-id "file-stream"
                  :version 1
                  :state '(:state 7)
                  :metadata '(:source :test)
                  :timestamp 100)))
         (close-file-event-store store)))
     (let ((recovered (make-file-event-store :path path)))
       (unwind-protect
            (progn
              (expect (event-store-current-version recovered "file-stream")
                      :to-be
                      1)
              (expect (event-store-current-global-position recovered)
                      :to-be
                      1)
              (expect (mapcar #'domain-event-id
                              (event-store-read-all recovered))
                      :to-equal
                      '("file-event-1"))
              (let ((snapshot
                      (event-store-read-snapshot recovered "file-stream")))
                (expect (event-snapshot-version snapshot) :to-be 1)
                (expect (event-snapshot-state snapshot)
                        :to-equal
                        '(:state 7))))
     (close-file-event-store recovered)))))))

 (it
  "persists and validates a non-zero global position origin"
  (call-with-durable-test-path
   "event-log-global-origin"
   (lambda (path)
     (let ((store (make-file-event-store
                   :path path
                   :global-position-start 40)))
       (unwind-protect
            (progn
              (event-store-append
               store
               "origin-stream"
               (list (make-test-event
                      "origin-event-1"
                      "origin-stream"
                      :value))
               :expected-version
               :no-stream)
              (expect (event-store-current-global-position store)
                      :to-be
                      41))
         (close-file-event-store store)))
     (let ((recovered (make-file-event-store :path path)))
       (unwind-protect
            (progn
              (expect (event-store-current-global-position recovered)
                      :to-be
                      41)
              (expect (domain-event-global-position
                       (first (event-store-read-all recovered)))
                      :to-be
                      41))
         (close-file-event-store recovered)))
     (signals event-sourcing-error
       (make-file-event-store
        :path path
        :global-position-start 41)))))

 (it
  "survives a truncated final record and preserves the retention boundary"
  (call-with-durable-test-path
   "event-log-retention"
   (lambda (path)
     (let ((store (make-file-event-store
                   :path path
                   :global-position-start 0)))
       (unwind-protect
            (progn
              (dotimes (index 3)
                (event-store-append
                 store
                 "retention-stream"
                 (list
                  (make-test-event
                   (format nil "retention-event-~D" (1+ index))
                   "retention-stream"
                   index))
                 :expected-version
                 (if (zerop index) :no-stream index)))
              (expect (event-store-prune
                       store
                       :before-global-position
                       2)
                      :to-be
                      2)
              (expect (event-store-retention-floor store) :to-be 2)
              (signals event-store-retention-gap
                (event-store-read-all store :after-global-position 0))
              (expect (mapcar #'domain-event-id
                              (event-store-read-all
                               store
                               :after-global-position
                               2))
                      :to-equal
                      '("retention-event-3")))
         (close-file-event-store store)))
     (with-open-file (stream path :direction :output :if-exists :append)
       (write-string "(:incomplete" stream))
     (let ((recovered (make-file-event-store :path path)))
       (unwind-protect
            (progn
              (expect (event-store-current-global-position recovered)
                      :to-be
                      3)
              (expect (event-store-retention-floor recovered) :to-be 2)
              (signals event-store-retention-gap
                (event-store-read-all recovered :after-global-position 0))
              (event-store-append
               recovered
               "retention-stream"
               (list (make-test-event
                      "retention-event-4"
                      "retention-stream"
                      3))
               :expected-version
               3)
              (expect (event-store-current-global-position recovered)
                      :to-be
                      4))
         (close-file-event-store recovered)))
     (let ((recovered-again (make-file-event-store :path path)))
       (unwind-protect
         (expect (mapcar #'domain-event-id
                            (event-store-read-all
                             recovered-again
                             :after-global-position
                             2))
                    :to-equal
                    '("retention-event-3" "retention-event-4"))
         (close-file-event-store recovered-again))))))

 (it
  "replaces an incomplete tail before reusing an open journal"
  (call-with-durable-test-path
   "event-log-open-tail"
   (lambda (path)
     (let ((store (make-file-event-store :path path)))
       (unwind-protect
            (progn
              (event-store-append
               store
               "open-tail-stream"
               (list (make-test-event
                      "open-tail-event-1"
                      "open-tail-stream"
                      :first))
               :expected-version
               :no-stream)
              (with-open-file (stream path :direction :output :if-exists :append)
                (write-string "(:incomplete" stream))
              (recover-file-event-store store)
              (event-store-append
               store
               "open-tail-stream"
               (list (make-test-event
                      "open-tail-event-2"
                      "open-tail-stream"
                      :second))
               :expected-version
               1)
              (expect (event-store-current-global-position store)
                      :to-be
                      2))
         (close-file-event-store store)))
     (let ((recovered (make-file-event-store :path path)))
       (unwind-protect
            (expect (mapcar #'domain-event-id
                            (event-store-read-all recovered))
                    :to-equal
                    '("open-tail-event-1" "open-tail-event-2"))
         (close-file-event-store recovered))))))

 (it
  "rejects malformed durable log records"
  (call-with-durable-test-path
   "event-log-corruption"
   (lambda (path)
     (with-open-file (stream path :direction :output :if-exists :supersede)
       (write-line "(:not-a-durable-record)" stream))
     (signals durable-store-corruption
       (make-file-event-store :path path)))))

 (it
 "prunes in-memory events at the same global boundary"
 (let ((store (make-in-memory-event-store :global-position-start 0)))
    (expect (event-store-retention-supported-p store) :to-be-truthy)
    (dotimes (index 2)
      (event-store-append
       store
       "memory-stream"
       (list
        (make-test-event
         (format nil "memory-event-~D" (1+ index))
         "memory-stream"
         index))
       :expected-version
       (if (zerop index) :no-stream index)))
    (expect (event-store-prune
             store
             :before-global-position
             2)
            :to-be
            2)
    (expect (event-store-retention-floor store) :to-be 2)
    (signals event-store-retention-gap
      (event-store-read-all store :after-global-position 0))
    (expect (length (event-store-read-all store :after-global-position 1))
            :to-be
            1)
    (expect (event-store-prune store) :to-be 0)
    (signals type-error
      (event-store-prune store :before-global-position 4)))))

(describe
 "durable subscriptions"
 (it
  "provides at-least-once delivery and persists consumer offsets"
  (call-with-durable-test-path
   "subscription-offsets"
   (lambda (path)
     (let ((store (make-in-memory-event-store :global-position-start 0)))
       (dotimes (index 3)
         (event-store-append
          store
          "subscription-stream"
          (list
           (make-test-event
            (format nil "subscription-event-~D" (1+ index))
            "subscription-stream"
            index))
          :expected-version
          (if (zerop index) :no-stream index)))
       (let* ((offsets (make-file-subscription-offset-store :path path))
              (subscription
                (make-subscription
                 :event-store store
                 :offset-store offsets
                 :consumer-id "consumer-1"
                 :batch-size 2))
              (first-delivery (subscription-poll subscription)))
         (expect (length (subscription-delivery-events first-delivery))
                 :to-be
                 2)
         (signals error
           (deliver-subscription
            subscription
            (lambda (event)
              (declare (ignore event))
              (error "simulated consumer failure"))))
         (expect (subscription-poll subscription) :to-be first-delivery)
         (multiple-value-bind (delivery count)
             (deliver-subscription
              subscription
              (lambda (event)
                (declare (ignore event))))
           (expect delivery :to-be first-delivery)
           (expect count :to-be 2))
         (expect (subscription-offset offsets "consumer-1") :to-be 2)
         (let* ((restarted-offsets (make-file-subscription-offset-store
                                    :path path))
                (restarted
                  (make-subscription
                   :event-store store
                   :offset-store restarted-offsets
                   :consumer-id "consumer-1"
                   :batch-size 1)))
           (multiple-value-bind (delivery count)
               (deliver-subscription
                restarted
                (lambda (event)
                  (expect (domain-event-id event)
                          :to-equal
                          "subscription-event-3")))
             (expect count :to-be 1)
             (expect (length (subscription-delivery-events delivery))
                     :to-be
                     1)))))))))

 (it
  "advances filtered subscriptions across excluded events"
  (let ((store (make-in-memory-event-store :global-position-start 0)))
    (dotimes (index 3)
      (event-store-append
       store
       "filtered-stream"
       (list
        (make-test-event
         (format nil "filtered-event-~D" (1+ index))
         "filtered-stream"
         index))
       :expected-version
       (if (zerop index) :no-stream index)))
    (let* ((offsets (make-in-memory-subscription-offset-store))
           (subscription
             (make-subscription
              :event-store store
              :offset-store offsets
              :consumer-id "filtered-consumer"
              :filter (lambda (event)
                        (= (domain-event-payload event) 2)))))
      (multiple-value-bind (delivery count)
          (deliver-subscription subscription (lambda (event)
                                              (expect (domain-event-payload event)
                                                      :to-be
                                                      2)))
        (expect count :to-be 1)
        (expect (length (subscription-delivery-events delivery)) :to-be 1))
      (expect (subscription-offset offsets "filtered-consumer") :to-be 3))))

(describe
 "durable outbox"
 (it
  "supports idempotency, leasing, fencing, retry, and file restart"
  (call-with-durable-test-path
   "outbox"
   (lambda (path)
     (let* ((message
              (make-outbox-message
               :id "message-1"
               :topic :email
               :payload '(:to "test@example.invalid")
               :metadata '(:kind :welcome)
               :created-at 0
               :available-at 0))
            (store (make-in-memory-outbox-store)))
       (expect (outbox-message-id (outbox-append store message))
               :to-be
               "message-1")
       (expect (outbox-message-id (outbox-append store message))
               :to-be
               "message-1")
       (signals outbox-message-conflict
         (outbox-append
          store
          (make-outbox-message
           :id "message-1"
           :topic :email
           :payload '(:to "other@example.invalid")
           :created-at 0
           :available-at 0)))
       (let* ((claimed (first (outbox-claim store :now 10 :lease-seconds 5)))
              (token (outbox-message-claim-token claimed)))
         (expect (outbox-message-status claimed) :to-be :in-flight)
         (signals event-sourcing-error
           (outbox-ack store "message-1" :claim-token 'stale-token))
         (outbox-fail
          store
          "message-1"
          :now 10
          :backoff-seconds 3
          :claim-token token)
         (expect (outbox-read-pending store :now 10) :to-be nil)
         (expect (length (outbox-read-pending store :now 13)) :to-be 1)
         (let ((reclaimed (first (outbox-claim
                                  store
                                  :now 13
                                  :lease-seconds 5))))
           (outbox-ack
            store
            "message-1"
            :claim-token (outbox-message-claim-token reclaimed)))
         (expect (outbox-read-pending store :now 20) :to-be nil))
       (outbox-append store message)
       (let ((file-store (make-file-outbox-store :path path)))
         (unwind-protect
              (progn
                (outbox-append
                 file-store
                 (make-outbox-message
                  :id "message-2"
                  :topic :audit
                  :payload '(:ok t)
                  :created-at 1
                  :available-at 1))
                (let ((restarted (make-file-outbox-store :path path)))
                  (expect (mapcar #'outbox-message-id
                                  (outbox-read-pending restarted :now 1))
                          :to-equal
                          '("message-2"))))
           (values)))))))

 (it
  "tracks permanent failures and permits explicit dead-letter requeue"
  (call-with-durable-test-path
   "outbox-dead-letter"
   (lambda (path)
     (dolist (store (list (make-in-memory-outbox-store)
                          (make-file-outbox-store :path path)))
       (outbox-append
        store
        (make-outbox-message
         :id "dead-letter-message"
         :topic :email
         :payload '(:to "test@example.invalid")
         :created-at 0
         :available-at 0))
       (multiple-value-bind (delivered claimed dead-lettered)
           (outbox-dispatch
            store
            (lambda (message)
              (declare (ignore message))
              (error "permanent delivery failure"))
            :now 0
            :max-attempts 1)
         (expect delivered :to-be 0)
         (expect claimed :to-be 1)
         (expect dead-lettered :to-be 1))
       (let ((dead-letter (outbox-read store "dead-letter-message")))
         (expect (outbox-message-status dead-letter) :to-be :dead-letter)
         (expect (stringp (outbox-message-last-error dead-letter)) :to-be t)
         (expect (search "permanent delivery failure"
                         (outbox-message-last-error dead-letter))
                 :to-be
                 0)
         (expect (outbox-message-dead-lettered-at dead-letter) :to-be 0))
       (expect (outbox-read-pending store :now 0) :to-be nil)
       (expect (length (outbox-read-all store :status :dead-letter)) :to-be 1)
       (when (file-outbox-store-p store)
         (let ((restarted (make-file-outbox-store :path path)))
           (expect (outbox-message-status
                    (outbox-read restarted "dead-letter-message"))
                   :to-be
                   :dead-letter)))
       (let ((requeued (outbox-requeue store
                                       "dead-letter-message"
                                       :available-at 5)))
         (expect (outbox-message-status requeued) :to-be :pending)
         (expect (outbox-message-last-error requeued) :to-be nil)
         (expect (outbox-message-dead-lettered-at requeued) :to-be nil))
       (expect (outbox-read-pending store :now 4) :to-be nil)
       (expect (length (outbox-read-pending store :now 5)) :to-be 1)
       (let ((claimed (first (outbox-claim store :now 5))))
         (expect (outbox-message-status
                  (outbox-ack store
                              (outbox-message-id claimed)
                              :claim-token
                              (outbox-message-claim-token claimed)))
                 :to-be
                 :delivered))
       (when (file-outbox-store-p store)
         (let ((restarted (make-file-outbox-store :path path)))
           (expect (outbox-message-status
                    (outbox-read restarted "dead-letter-message"))
                   :to-be
                   :delivered)))))))

 (it
  "rolls back the in-memory event and outbox together"
  (let* ((event-store (make-in-memory-event-store :global-position-start 0))
         (outbox-store (make-in-memory-outbox-store))
         (store (make-in-memory-event-outbox-store
                 :event-store event-store
                 :outbox-store outbox-store))
         (event-1 (make-test-event "atomic-event-1" "atomic-stream" 1))
         (message-1 (make-outbox-message
                     :id "atomic-message-1"
                     :topic :audit
                     :payload '(:event 1)
                     :created-at 0
                     :available-at 0)))
    (event-store-append-with-outbox
     store
     "atomic-stream"
     (list event-1)
     (list message-1)
     :expected-version
     :no-stream)
    (signals outbox-message-conflict
      (event-store-append-with-outbox
       store
       "atomic-stream"
       (list (make-test-event "atomic-event-2" "atomic-stream" 2))
       (list
        (make-outbox-message
         :id "atomic-message-1"
         :topic :audit
         :payload '(:event 999)
         :created-at 0
         :available-at 0))
       :expected-version
       1))
    (expect (event-store-current-version event-store "atomic-stream") :to-be 1)
    (expect (length (event-store-read-all event-store)) :to-be 1)
    (expect (length (outbox-read-pending outbox-store :now 0)) :to-be 1)))
 (it
  "reports when an event store lacks atomic outbox support"
  (let ((store (make-in-memory-event-store :global-position-start 0)))
    (signals event-store-operation-not-supported
      (event-store-append-with-outbox
       store
       "unsupported-atomic-stream"
       nil
       nil)))))

(describe
 "durable projection checkpoints"
 (it
  "resumes a projection from a durable checkpoint"
  (call-with-durable-test-path
   "projection-checkpoint"
   (lambda (path)
     (let ((event-store (make-in-memory-event-store :global-position-start 0)))
       (dotimes (index 3)
         (event-store-append
         event-store
          "projection-stream"
          (list
           (make-test-event
            (format nil "projection-event-~D" (1+ index))
            "projection-stream"
            (1+ index)))
          :expected-version
          (if (zerop index) :no-stream index)))
       (let* ((checkpoint-store (make-file-projection-checkpoint-store
                                 :path path))
              (projection
                (make-projection
                 :initial-state 0
                 :handler (lambda (state event)
                            (+ state (domain-event-payload event)))))
              (runner
                (make-durable-projection-runner
                 :projection projection
                 :event-store event-store
                 :checkpoint-store checkpoint-store
                 :checkpoint-key "sum")))
         (multiple-value-bind (state position)
             (run-projection-once runner :limit 2)
           (expect state :to-be 3)
           (expect position :to-be 2))
         (let* ((restarted-checkpoints
                  (make-file-projection-checkpoint-store :path path))
                (restarted-projection
                  (make-projection
                   :initial-state 100
                   :handler (lambda (state event)
                              (+ state (domain-event-payload event)))))
                (restarted
                  (make-durable-projection-runner
                   :projection restarted-projection
                   :event-store event-store
                   :checkpoint-store restarted-checkpoints
                   :checkpoint-key "sum")))
           (multiple-value-bind (state position)
               (run-projection-once restarted)
             (expect state :to-be 6)
             (expect position :to-be 3))
           (let ((record (projection-checkpoint-load
                          restarted-checkpoints
                          "sum")))
             (expect (projection-checkpoint-record-state record) :to-be 6)
             (expect (projection-checkpoint-record-position record) :to-be 3))))))))

 (it
  "does not advance a checkpoint when projection handling fails"
  (let ((event-store (make-in-memory-event-store :global-position-start 0)))
    (dotimes (index 2)
      (event-store-append
       event-store
       "failure-stream"
       (list
        (make-test-event
         (format nil "failure-event-~D" (1+ index))
         "failure-stream"
         (1+ index)))
       :expected-version
       (if (zerop index) :no-stream index)))
    (let* ((checkpoints (make-in-memory-projection-checkpoint-store))
           (projection
             (make-projection
              :initial-state 0
              :handler (lambda (state event)
                         (if (= (domain-event-payload event) 2)
                             (error "projection failed")
                           (+ state (domain-event-payload event))))))
           (runner
             (make-durable-projection-runner
              :projection projection
              :event-store event-store
              :checkpoint-store checkpoints
              :checkpoint-key "failing")))
      (signals projection-failure (run-projection-once runner))
      (let ((record (projection-checkpoint-load checkpoints "failing")))
        (expect (projection-checkpoint-record-state record) :to-be 0)
        (expect (projection-checkpoint-record-position record) :to-be 0)))))

 (it
  "restores mutable projection state with a state copier"
  (let ((event-store (make-in-memory-event-store :global-position-start 0)))
    (dotimes (index 2)
      (event-store-append
       event-store
       "mutable-failure-stream"
       (list
        (make-test-event
         (format nil "mutable-failure-event-~D" (1+ index))
         "mutable-failure-stream"
         (1+ index)))
       :expected-version
       (if (zerop index) :no-stream index)))
    (let* ((checkpoints (make-in-memory-projection-checkpoint-store))
           (projection
             (make-projection
              :initial-state (list 0)
              :handler (lambda (state event)
                         (incf (first state)
                               (domain-event-payload event))
                         (when (= (domain-event-payload event) 2)
                           (error "mutable projection failed"))
                         state)))
           (runner
             (make-durable-projection-runner
              :projection projection
              :event-store event-store
              :checkpoint-store checkpoints
              :checkpoint-key "mutable-failing"
              :state-copy #'copy-tree)))
      (multiple-value-bind (state position)
          (run-projection-once runner :limit 1)
        (expect state :to-equal '(1))
        (expect position :to-be 1))
      (signals projection-failure (run-projection-once runner))
      (expect (projection-state projection) :to-equal '(1))
      (let ((record
              (projection-checkpoint-load checkpoints "mutable-failing")))
        (expect (projection-checkpoint-record-state record)
                :to-equal '(1))
        (expect (projection-checkpoint-record-position record)
                :to-be 1)))))
  )

(describe
 "event upcasters and operations"
 (it
  "applies an ordered schema-upcaster chain and rejects duplicates"
  (let* ((registry (make-upcaster-registry))
         (event
           (make-test-event
            "upcast-event"
            "upcast-stream"
            1
            :type :order
            :schema-version 1))
         (make-versioned
           (lambda (event payload version)
             (make-domain-event
              :id (domain-event-id event)
              :type (domain-event-type event)
              :stream-id (domain-event-stream-id event)
              :aggregate-id (domain-event-aggregate-id event)
              :payload payload
              :metadata (domain-event-metadata event)
              :timestamp (domain-event-timestamp event)
              :schema-version version
              :version (domain-event-version event)
              :correlation-id (domain-event-correlation-id event)
              :causation-id (domain-event-causation-id event)
              :global-position (domain-event-global-position event)))))
    (register-upcaster
     registry
     :order
     1
     2
     (lambda (current)
       (funcall make-versioned current (1+ (domain-event-payload current)) 2)))
    (register-upcaster
     registry
     :order
     2
     3
     (lambda (current)
       (funcall make-versioned current (1+ (domain-event-payload current)) 3)))
    (let ((upcasted (funcall (upcaster-registry-function registry) event)))
      (expect (domain-event-payload upcasted) :to-be 3)
      (expect (domain-event-schema-version upcasted) :to-be 3)
      (expect (domain-event-global-position upcasted)
              :to-be
              (domain-event-global-position event)))
    (signals event-sourcing-error
      (register-upcaster
       registry
       :order
       1
       2
       #'identity))))

 (it
  "rejects malformed upcaster versions and envelope changes"
  (let ((event
          (make-domain-event
           :id "contract-event"
           :type :contract
           :stream-id "contract-stream"
           :aggregate-id "contract-aggregate"
           :payload '(:value 1)
           :metadata '(:source :test)
           :timestamp 100
           :schema-version 1
           :version 4
           :correlation-id "correlation-1"
           :causation-id "causation-1"
           :global-position 8)))
    (signals event-sourcing-error
      (register-upcaster
       (make-upcaster-registry)
       :contract
       "not-an-integer"
       2
       #'identity))
    (signals event-sourcing-error
      (register-upcaster
       (make-upcaster-registry)
       :contract
       1
       "not-an-integer"
       #'identity))
    (labels ((variant (&key id type stream-id aggregate-id metadata timestamp
                            version correlation-id causation-id global-position)
               (make-domain-event
                :id (or id (domain-event-id event))
                :type (or type (domain-event-type event))
                :stream-id (or stream-id (domain-event-stream-id event))
                :aggregate-id (or aggregate-id
                                  (domain-event-aggregate-id event))
                :payload (domain-event-payload event)
                :metadata (or metadata (domain-event-metadata event))
                :timestamp (or timestamp (domain-event-timestamp event))
                :schema-version (domain-event-schema-version event)
                :version (or version (domain-event-version event))
                :correlation-id (or correlation-id
                                    (domain-event-correlation-id event))
                :causation-id (or causation-id
                                  (domain-event-causation-id event))
                :global-position (or global-position
                                     (domain-event-global-position event)))))
      (dolist (after
                (list
                 (variant :id "changed-id")
                 (variant :type :changed-type)
                 (variant :stream-id "changed-stream")
                 (variant :aggregate-id "changed-aggregate")
                 (variant :metadata '(:source :changed))
                 (variant :timestamp 101)
                 (variant :version 5)
                 (variant :correlation-id "changed-correlation")
                 (variant :causation-id "changed-causation")
                 (variant :global-position 9)))
        (expect
         (cl-event-sourcing-kit::%upcaster-envelope-preserved-p event after)
         :to-be
         nil))
      (let ((registry (make-upcaster-registry)))
        (register-upcaster
         registry
         :contract
         1
         2
         (lambda (current)
           (declare (ignore current))
           (make-domain-event
            :id "contract-event"
            :type :contract
            :stream-id "contract-stream"
            :aggregate-id "contract-aggregate"
            :payload '(:value 2)
            :metadata '(:source :test)
            :timestamp 100
            :schema-version 3
            :version 4
            :correlation-id "correlation-1"
            :causation-id "causation-1"
            :global-position 8)))
        (signals event-sourcing-error
          (funcall (upcaster-registry-function registry) event)))
      (let ((registry (make-upcaster-registry)))
        (register-upcaster
         registry
         :contract
         1
         2
         (lambda (current)
           (make-domain-event
            :id "changed-id"
            :type (domain-event-type current)
            :stream-id (domain-event-stream-id current)
            :aggregate-id (domain-event-aggregate-id current)
            :payload '(:value 2)
            :metadata (domain-event-metadata current)
            :timestamp (domain-event-timestamp current)
            :schema-version 2
            :version (domain-event-version current)
            :correlation-id (domain-event-correlation-id current)
            :causation-id (domain-event-causation-id current)
            :global-position (domain-event-global-position current))))
        (signals event-sourcing-error
          (funcall (upcaster-registry-function registry) event))))))

 (it
  "reports event-store operations and retries transient failures"
  (let* ((before nil)
         (after nil)
         (errors nil)
         (store (make-observed-event-store
                (make-in-memory-event-store :global-position-start 0)
                :before (lambda (operation) (push operation before))
                :after (lambda (operation) (push operation after))
                :on-error (lambda (operation condition)
                            (declare (ignore condition))
                            (push operation errors)))))
    (event-store-append
     store
     "observed-stream"
     (list (make-test-event "observed-event-1" "observed-stream" 1))
     :expected-version
     :no-stream)
    (expect (member :append before) :to-be-truthy)
    (expect (member :append after) :to-be-truthy)
    (handler-case
        (event-store-append
         store
         "observed-stream"
         (list (make-test-event "observed-event-2" "observed-stream" 2))
         :expected-version
         :no-stream)
      (event-version-conflict () nil))
    (expect (member :append errors) :to-be-truthy)
    (let* ((attempts 0)
           (policy
             (resilience-kit:make-retry-policy
              :max-attempts 3
              :retry-safe-p t
              :condition-classifier
              (lambda (condition attempt)
                (declare (ignore condition attempt))
                t))))
      (expect
       (resilience-kit:with-retry (policy)
         (incf attempts)
         (if (< attempts 3)
             (error "transient")
           :ok))
       :to-be
       :ok)
      (expect attempts :to-be 3))
    (let ((policy
            (resilience-kit:make-retry-policy
             :max-attempts 2
             :retry-safe-p t
             :condition-classifier
             (lambda (condition attempt)
               (declare (ignore condition attempt))
               t))))
      (signals error
        (resilience-kit:with-retry (policy)
          (error "permanent"))))))
 )
