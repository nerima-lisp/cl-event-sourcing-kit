(in-package #:cl-event-sourcing-kit/test)

(defun write-durable-edge-lines (path lines)
  (with-open-file (stream path :direction :output :if-exists :supersede)
    (dolist (line lines)
      (write-line line stream))))

(defun write-durable-edge-value (path value &key serializer)
  (write-durable-edge-lines
   path
   (list (serialize-value value :serializer serializer))))

(defun write-durable-edge-log (path records)
  (write-durable-edge-lines
   path
   (mapcar #'serialize-value records)))

(defun write-durable-edge-readable-log (path records)
  (write-durable-edge-lines
   path
   (mapcar (lambda (record)
             (with-standard-io-syntax
               (let ((*print-pretty* nil))
                 (write-to-string record :readably t))))
           records)))

(defun write-durable-edge-readable-log-with-tail (path records tail)
  (with-open-file (stream path :direction :output :if-exists :supersede)
    (dolist (record records)
      (with-standard-io-syntax
        (let ((*print-pretty* nil))
          (write-line (write-to-string record :readably t) stream))))
    (write-string tail stream)))

(defun edge-event-with-schema (event schema-version &key payload stream-id)
  (make-domain-event
   :id (domain-event-id event)
   :type (domain-event-type event)
   :stream-id (or stream-id (domain-event-stream-id event))
   :aggregate-id (domain-event-aggregate-id event)
   :payload (if (eq payload cl-event-sourcing-kit::*unspecified*)
                (domain-event-payload event)
              payload)
   :metadata (domain-event-metadata event)
   :timestamp (domain-event-timestamp event)
   :schema-version schema-version
   :version (domain-event-version event)
   :correlation-id (domain-event-correlation-id event)
   :causation-id (domain-event-causation-id event)
   :global-position (domain-event-global-position event)))

(defclass edge-checkpoint-store (projection-checkpoint-store)
  ((record :initform nil :accessor edge-checkpoint-record)
   (fail-next-save-p :initform nil :accessor edge-fail-next-checkpoint-save-p)))

(defmethod projection-checkpoint-load ((store edge-checkpoint-store) key)
  (declare (ignore key))
  (let ((record (edge-checkpoint-record store)))
    (and record
         (make-projection-checkpoint-record
          :state (projection-checkpoint-record-state record)
          :position (projection-checkpoint-record-position record)
          :updated-at (projection-checkpoint-record-updated-at record)))))

(defmethod projection-checkpoint-save
    ((store edge-checkpoint-store) key record)
  (declare (ignore key))
  (when (edge-fail-next-checkpoint-save-p store)
    (setf (edge-fail-next-checkpoint-save-p store) nil)
    (error "edge checkpoint failure"))
  (setf (edge-checkpoint-record store)
        (make-projection-checkpoint-record
         :state (projection-checkpoint-record-state record)
         :position (projection-checkpoint-record-position record)
         :updated-at (projection-checkpoint-record-updated-at record)))
  record)

(defmethod projection-checkpoint-delete ((store edge-checkpoint-store) key)
  (declare (ignore key))
  (prog1 (not (null (edge-checkpoint-record store)))
    (setf (edge-checkpoint-record store) nil)))

(describe
 "durable edge model contracts"
 (it
  "covers default constructors and durable predicates"
  (let* ((serializer (make-event-serializer))
         (default-serializer (apply #'make-event-serializer nil))
         (outbox-message
           (make-outbox-message
            :id "edge-message"
            :topic "edge-topic"
            :payload '(:value 1)
            :created-at 10))
         (default-outbox-message
           (apply #'make-outbox-message
                  '(:id "default-message"
                    :topic "default-topic"
                    :payload :payload)))
         (event-store (make-in-memory-event-store))
         (outbox-store (make-in-memory-outbox-store))
         (combined (make-in-memory-event-outbox-store))
         (checkpoint-record
           (make-projection-checkpoint-record :position 0))
         (checkpoint-store (make-in-memory-projection-checkpoint-store))
         (offset-store (make-in-memory-subscription-offset-store)))
    (expect (event-serializer-p serializer) :to-be-truthy)
    (expect (event-serializer-p default-serializer) :to-be-truthy)
    (expect (serialize-value '(:ready t) :serializer serializer)
            :to-equal
            "(:READY T)")
    (expect (outbox-message-created-at outbox-message) :to-be 10)
    (expect (outbox-message-status default-outbox-message) :to-be :pending)
    (expect (outbox-message-attempts default-outbox-message) :to-be 0)
    (expect (outbox-message-status outbox-message) :to-be :pending)
    (expect (outbox-message-attempts outbox-message) :to-be 0)
    (expect (outbox-message-available-at outbox-message) :to-be 10)
    (expect (event-outbox-store-p combined) :to-be-truthy)
    (expect (in-memory-outbox-store-p outbox-store) :to-be-truthy)
    (expect (in-memory-event-store-p event-store) :to-be-truthy)
    (expect (projection-checkpoint-record-p checkpoint-record) :to-be-truthy)
    (expect (projection-checkpoint-store-p checkpoint-store) :to-be-truthy)
    (expect (in-memory-projection-checkpoint-store-p checkpoint-store)
            :to-be-truthy)
    (expect (subscription-offset-store-p offset-store) :to-be-truthy)
    (expect (in-memory-subscription-offset-store-p offset-store) :to-be-truthy)
    (expect (file-event-store-p nil) :to-be nil)
    (expect (file-subscription-offset-store-p nil) :to-be nil)
    (expect (file-outbox-store-p nil) :to-be nil)
    (expect (file-projection-checkpoint-store-p nil) :to-be nil)
    (expect (durable-projection-runner-p nil) :to-be nil)
    (expect (upcaster-registry-p nil) :to-be nil)
    (expect (event-outbox-store-p nil) :to-be nil)
    (signals error (make-outbox-message :id "edge-message"))
    (signals error (make-outbox-message :topic "edge-topic"))
    (signals error (make-outbox-message :id "edge-message" :topic "edge-topic"
                                        :attempts -1))
    (signals error (make-outbox-message :id "edge-message" :topic "edge-topic"
                                        :status :unknown))
    (signals error (make-outbox-message :id "edge-message" :topic "edge-topic"
                                        :last-error 1))
    (signals error (make-outbox-message :id "edge-message" :topic "edge-topic"
                                        :dead-lettered-at "bad"))
    (signals error (make-projection-checkpoint-record))
    (signals error (make-projection-checkpoint-record :position -1)))))

(describe
 "durable edge upcasters"
 (it
  "validates registries, preserves envelopes, and detects cycles"
  (let* ((event
           (make-test-event
            "upcast-edge"
            "upcast-stream"
            '(:old t)
            :type :edge-event
            :schema-version 1
            :version 3
            :global-position 8
            :aggregate-id "aggregate"
            :metadata '(:source :edge)
            :correlation-id "correlation"
            :causation-id "causation"
            :timestamp 100))
         (registry (make-upcaster-registry))
         (upcaster
           (lambda (value)
             (edge-event-with-schema value 2 :payload '(:new t))))
         (function (progn
                     (register-upcaster registry :edge-event 1 2 upcaster)
                     (upcaster-registry-function registry
                                                  :event-type :edge-event))))
    (expect (domain-event-schema-version (funcall function event)) :to-be 2)
    (expect (domain-event-payload (funcall function event))
            :to-equal
            '(:new t))
    (expect (funcall (upcaster-registry-function registry :event-type :other)
                     event)
            :to-be event)
    (expect (funcall (upcaster-registry-function registry) event)
            :to-be-truthy)
    (signals error (funcall function nil))
    (signals error (make-upcaster-registry :table 1))
    (signals error
      (make-upcaster-registry :table (make-hash-table :test #'eql)))
    (signals error (register-upcaster registry nil 1 2 upcaster))
    (signals error (register-upcaster registry :other -1 2 upcaster))
    (signals error (register-upcaster registry :other 1 -1 upcaster))
    (signals error (register-upcaster registry :other 2 2 upcaster))
    (signals error (register-upcaster registry :other 1 2 1))
    (signals error (register-upcaster registry :edge-event 1 2 upcaster))
    (signals error
      (upcaster-registry-function
       registry
       :event-type cl-event-sourcing-kit::*unspecified*)))
  (let* ((event (make-test-event "bad-upcast" "upcast" nil
                                :type :bad-upcast :schema-version 1))
         (registry (make-upcaster-registry)))
    (register-upcaster registry :bad-upcast 1 2 (lambda (value)
                                                   (declare (ignore value))
                                                   1))
    (signals error
      (funcall (upcaster-registry-function registry) event)))
  (let* ((event (make-test-event "mismatch-upcast" "upcast" nil
                                :type :mismatch :schema-version 1))
         (registry (make-upcaster-registry)))
    (register-upcaster registry :mismatch 1 2
                       (lambda (value)
                         (edge-event-with-schema value 2
                                                  :stream-id "wrong-stream")))
    (signals error
      (funcall (upcaster-registry-function registry) event)))
  (let* ((event (make-test-event "cycle-upcast" "upcast" nil
                                :type :cycle :schema-version 1))
         (registry (make-upcaster-registry)))
    (register-upcaster registry :cycle 1 2
                       (lambda (value) (edge-event-with-schema value 2)))
    (setf (gethash (list :cycle 2)
                   (cl-event-sourcing-kit::%upcaster-table registry))
          (cons 1
                (lambda (value) (edge-event-with-schema value 1))))
    (signals error
      (funcall (upcaster-registry-function registry) event)))))

(describe
 "observed event-store edge contracts"
 (it
  "observes every event-store operation and tolerates callback failures"
  (let* ((delegate (make-in-memory-event-store :global-position-start 0))
         (before nil)
         (after nil)
         (errors nil)
         (observed
           (make-observed-event-store
            delegate
            :before (lambda (&rest arguments)
                      (push arguments before))
            :after (lambda (&rest arguments)
                     (push arguments after))
            :on-error (lambda (&rest arguments)
                        (push arguments errors))))
         (event (make-test-event "observed-1" "observed" '(:value 1)))
         (request
           (make-event-append-request
            :stream-id "observed-batch"
            :events (list (make-test-event "observed-2" "observed-batch" 2))
            :expected-version :no-stream))
         (snapshot
           (make-event-snapshot
            :stream-id "observed"
            :version 1
            :state '(:value 1)
            :timestamp 100)))
    (multiple-value-bind (events version)
        (event-store-append observed "observed" (list event))
      (expect version :to-be 1)
      (expect (event-store-event-equivalent-p observed (first events) event)
              :to-be-truthy))
    (expect (event-store-read observed "observed") :to-be-truthy)
    (expect (event-store-read-all observed) :to-be-truthy)
    (expect (event-store-current-version observed "observed") :to-be 1)
    (expect (event-store-current-global-position observed) :to-be 1)
    (expect (event-store-stream-exists-p observed "observed") :to-be-truthy)
    (expect (event-store-global-position-supported-p observed) :to-be-truthy)
    (expect (event-store-event-equivalent-p observed event event) :to-be-truthy)
    (expect (length (event-store-append-batch observed (list request))) :to-be 1)
    (event-store-save-snapshot observed snapshot)
    (let ((read-snapshot (event-store-read-snapshot observed "observed")))
      (expect (event-snapshot-stream-id read-snapshot) :to-equal "observed")
      (expect (event-snapshot-version read-snapshot) :to-be 1)
      (expect (event-snapshot-state read-snapshot) :to-equal '(:value 1)))
    (event-store-delete-snapshot observed "observed")
    (expect (event-store-read-snapshot observed "observed") :to-be nil)
    (expect (event-store-snapshots-supported-p observed) :to-be-truthy)
    (expect (event-store-prune observed) :to-be 0)
    (expect (event-store-retention-supported-p observed) :to-be-truthy)
    (expect (event-store-retention-floor observed) :to-be 0)
    (signals error
      (event-store-append observed "observed"
                          (list (make-test-event "observed-conflict"
                                                 "observed"
                                                 nil))
                           :expected-version 99))
    (expect before :to-be-truthy)
    (expect after :to-be-truthy)
    (expect errors :to-be-truthy))
  (let* ((delegate (make-in-memory-event-store))
         (observed
           (make-observed-event-store
            delegate
            :before (lambda (&rest arguments)
                      (declare (ignore arguments))
                      (error "ignored before callback"))
            :after (lambda (&rest arguments)
                     (declare (ignore arguments))
                     (error "ignored after callback"))
            :on-error (lambda (&rest arguments)
                        (declare (ignore arguments))
                        (error "ignored error callback"))))
         (event (make-test-event "ignored-callback" "ignored" nil)))
    (event-store-append observed "ignored" (list event))
    (signals error
      (event-store-append observed "ignored"
                          (list (make-test-event "ignored-callback-2"
                                                 "ignored"
                                                 nil))
                          :expected-version 99)))))

(describe
 "durable projection edge contracts"
 (it
  "rolls back checkpoint failures and validates runner contracts"
  (let* ((event-store (make-in-memory-event-store :global-position-start 0))
         (checkpoint-store (make-instance 'edge-checkpoint-store))
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
            :checkpoint-key "edge-projection")))
    (event-store-append
     event-store
     "edge-projection"
     (list (make-test-event "projection-edge-1" "edge-projection" 2)))
    (setf (edge-fail-next-checkpoint-save-p checkpoint-store) t)
    (signals error (run-projection-once runner))
    (expect (projection-state projection) :to-be 0)
    (expect (projection-checkpoint projection) :to-be 0)
    (setf (edge-checkpoint-record checkpoint-store)
          (make-projection-checkpoint-record :state 9 :position 1))
    (let ((restored
            (make-projection
             :initial-state 0
             :handler (lambda (state event)
                        (+ state (domain-event-payload event))))))
      (make-durable-projection-runner
       :projection restored
       :event-store event-store
       :checkpoint-store checkpoint-store
       :checkpoint-key "edge-projection")
      (expect (projection-state restored) :to-be 9)
      (expect (projection-checkpoint restored) :to-be 1))
    (signals error (make-durable-projection-runner))
    (signals error
      (make-durable-projection-runner
       :projection projection
       :event-store nil
       :checkpoint-store checkpoint-store
       :checkpoint-key "edge-projection"))
    (signals error
      (make-durable-projection-runner
       :projection projection
       :event-store event-store
       :checkpoint-store nil
       :checkpoint-key "edge-projection"))
    (signals error
      (make-durable-projection-runner
       :projection projection
       :event-store event-store
       :checkpoint-store checkpoint-store
       :checkpoint-key nil))
    (signals error
      (make-durable-projection-runner
       :projection projection
       :event-store (make-instance 'unsupported-store)
       :checkpoint-store checkpoint-store
       :checkpoint-key "edge-projection"))
    (signals type-error
      (make-durable-projection-runner
       :projection projection
       :event-store event-store
       :checkpoint-store checkpoint-store
       :checkpoint-key "edge-projection"
       :state-copy 1))
    (signals error
      (make-durable-projection-runner
       :projection projection
       :event-store event-store
       :checkpoint-store checkpoint-store
       :checkpoint-key "edge-projection"
       :lock 1)))))

(describe
 "durable serialization edge contracts"
 (it
  "covers safe values, unicode limits, codecs, and wire helpers"
  (let ((serializer (make-event-serializer :max-bytes 1024 :max-depth 8)))
    (expect (deserialize-value (serialize-value "日本語😀" :serializer serializer)
                               :serializer serializer)
            :to-equal
            "日本語😀")
    (expect (cl-event-sourcing-kit::%utf-8-octet-length "ascii") :to-be 5)
    (expect (cl-event-sourcing-kit::%utf-8-octet-length "é") :to-be 2)
    (expect (cl-event-sourcing-kit::%utf-8-octet-length "€") :to-be 3)
    (expect (cl-event-sourcing-kit::%utf-8-octet-length "😀") :to-be 4)
    (expect (cl-event-sourcing-kit::%safe-symbol-package-p 'cl:car)
            :to-be-truthy)
    (expect (cl-event-sourcing-kit::%safe-symbol-package-p :keyword)
            :to-be-truthy)
    (expect (cl-event-sourcing-kit::%safe-symbol-package-p 'cl-user::edge)
            :to-be-truthy)
    (expect (cl-event-sourcing-kit::%safe-symbol-package-p
             'cl-event-sourcing-kit::edge)
            :to-be-truthy)
    (expect (cl-event-sourcing-kit::%safe-symbol-package-p
             (make-symbol "EDGE"))
            :to-be nil)
    (expect (cl-event-sourcing-kit::%safe-serializable-value-p #(1 2)
                                                               :max-depth 2)
            :to-be-truthy)
    (let ((vector (make-array 1)))
      (setf (aref vector 0) vector)
      (expect (cl-event-sourcing-kit::%safe-serializable-value-p vector)
              :to-be nil)
      (signals event-serialization-error (serialize-value vector)))
    (dolist (dispatch '("#." "#," "#'" "#s" "#p" "#c" "#a"))
      (expect (cl-event-sourcing-kit::%unsafe-reader-dispatch-p dispatch)
              :to-be-truthy))
    (signals event-serialization-error
      (serialize-value 1 :serializer
                       (make-event-serializer
                        :encode (lambda (value)
                                  (declare (ignore value))
                                  1))))
    (signals event-serialization-error
      (deserialize-value "value" :serializer
                         (make-event-serializer
                          :decode (lambda (value)
                                    (declare (ignore value))
                                    (error "codec failure")))))
    (signals event-serialization-error
      (deserialize-value "1 2" :serializer serializer))
    (signals event-serialization-error
      (deserialize-value "#.(+ 1 2)" :serializer serializer))
    (signals event-serialization-error
      (serialize-value "123456" :serializer
                       (make-event-serializer :max-bytes 2)))
    (signals type-error
      (cl-event-sourcing-kit::%safe-serializable-value-p
       :value
       :max-depth
       "not-an-integer")))
  (let ((event (make-test-event "wire-edge" "wire" '(:payload t))))
    (expect (domain-event-id (deserialize-domain-event
                              (serialize-domain-event event)))
            :to-equal
            "wire-edge")
    (signals error
      (cl-event-sourcing-kit::%domain-event-from-wire
       '(:domain-event :id "missing")))
    (signals error
      (cl-event-sourcing-kit::%domain-event-from-wire '(:unknown)))
    (signals error
      (cl-event-sourcing-kit::%snapshot-from-wire '(:unknown)))
    (signals error
      (cl-event-sourcing-kit::%snapshot-from-wire
       '(:event-snapshot :stream-id "wire")))
    (signals error
      (cl-event-sourcing-kit::%domain-event-from-wire
       (cons :domain-event 1)))
    (signals error
      (cl-event-sourcing-kit::%snapshot-from-wire
       (cons :event-snapshot 1)))
    (signals error
      (cl-event-sourcing-kit::%required-wire-value nil :id))
    (signals error
      (cl-event-sourcing-kit::%required-wire-value
       '(:id "present")
       :missing))
    (signals error
      (cl-event-sourcing-kit::%required-wire-value
       '(:wire :id "present" :dangling)
       :missing))
    (signals error
      (cl-event-sourcing-kit::%required-wire-value
       (cons :wire 1)
       :missing))
    (signals error
      (cl-event-sourcing-kit::%durable-pathname 1)))))

(describe
 "durable subscription edge contracts"
 (it
  "validates offset files and monotonic durable positions"
  (signals error (make-file-subscription-offset-store))
  (call-with-durable-test-path
   "subscription-invalid-sync"
   (lambda (path)
     (signals error
       (make-file-subscription-offset-store :path path :sync 1))))
  (let ((store (make-in-memory-subscription-offset-store)))
    (save-subscription-offset store "memory-edge" 3)
    (signals error (save-subscription-offset store "memory-edge" 2)))
  (call-with-durable-test-path
   "subscription-backwards"
   (lambda (path)
     (let ((store (make-file-subscription-offset-store :path path)))
       (save-subscription-offset store "file-edge" 3)
       (signals error (save-subscription-offset store "file-edge" 2)))))
  (call-with-durable-test-path
   "subscription-invalid-envelope"
   (lambda (path)
     (write-durable-edge-value path '(:invalid-envelope))
     (let ((store (make-file-subscription-offset-store :path path)))
       (signals durable-store-corruption
         (subscription-offset store "consumer")))))
  (call-with-durable-test-path
   "subscription-dotted-envelope"
   (lambda (path)
     (signals durable-store-corruption
       (cl-event-sourcing-kit::%subscription-offset-table-from-wire
        (cons :subscription-offsets 1)
        path))))
  (call-with-durable-test-path
   "subscription-dotted-record"
   (lambda (path)
     (signals durable-store-corruption
       (cl-event-sourcing-kit::%subscription-offset-table-from-wire
        (list :subscription-offsets
              (cons :subscription-offset 1))
        path))))
  (call-with-durable-test-path
   "subscription-invalid-record"
   (lambda (path)
     (write-durable-edge-value path '(:subscription-offsets (:invalid-record)))
     (let ((store (make-file-subscription-offset-store :path path)))
       (signals durable-store-corruption
         (subscription-offset store "consumer")))))
  (call-with-durable-test-path
   "subscription-duplicate-consumer"
   (lambda (path)
     (write-durable-edge-value
      path
      '(:subscription-offsets
        (:subscription-offset :consumer-id "consumer" :global-position 1)
        (:subscription-offset :consumer-id "consumer" :global-position 2)))
     (let ((store (make-file-subscription-offset-store :path path)))
       (signals durable-store-corruption
         (subscription-offset store "consumer")))))
  (call-with-durable-test-path
   "subscription-wrapped-corruption"
   (lambda (path)
     (write-durable-edge-value
      path
      '(:subscription-offsets
        (:subscription-offset :consumer-id "consumer" :global-position -1)))
     (let ((store (make-file-subscription-offset-store :path path)))
       (signals durable-store-corruption
         (subscription-offset store "consumer"))))))

 (it
  "validates subscription construction, filtering, and acknowledgement"
  (let* ((event-store (make-in-memory-event-store :global-position-start 0))
         (offset-store (make-in-memory-subscription-offset-store)))
    (signals error (make-subscription))
    (signals error
      (make-subscription :event-store event-store
                         :offset-store nil
                         :consumer-id "invalid-offset-store"))
    (signals error
      (make-subscription :event-store event-store
                         :offset-store offset-store))
    (signals error
      (make-subscription :event-store event-store
                         :offset-store offset-store
                         :consumer-id "invalid-batch"
                         :batch-size 0))
    (signals error
      (make-subscription :event-store event-store
                         :offset-store offset-store
                         :consumer-id "invalid-batch-type"
                         :batch-size "not-an-integer"))
    (signals error
      (make-subscription :event-store event-store
                         :offset-store offset-store
                         :consumer-id "invalid-filter"
                         :filter 1))
    (signals error
      (make-subscription :event-store (make-instance 'unsupported-store)
                         :offset-store offset-store
                         :consumer-id "unsupported-store"))
    (let ((subscription
            (make-subscription :event-store event-store
                               :offset-store offset-store
                               :consumer-id "defaults")))
      (expect (subscription-cursor subscription) :to-be 0)
      (expect (apply #'subscription-p (list subscription)) :to-be-truthy))
    (let ((subscription
            (apply #'make-subscription
                   (list :event-store event-store
                         :offset-store offset-store
                         :consumer-id "dynamic-defaults"))))
      (expect (cl-event-sourcing-kit::%subscription-batch-size subscription)
              :to-be
              100))
    (save-subscription-offset offset-store "cursor-edge" 5)
    (let ((subscription
            (make-subscription
             :event-store event-store
             :offset-store offset-store
             :consumer-id "cursor-edge"
             :cursor 5)))
      (expect (subscription-cursor subscription) :to-be 5))
    (signals error
      (make-subscription :event-store event-store
                         :offset-store offset-store
                         :consumer-id "cursor-edge"
                         :cursor 4))
    (let ((subscription
            (make-subscription
             :event-store event-store
             :offset-store offset-store
             :consumer-id "filter-edge"
             :filter (lambda (event)
                       (= (domain-event-payload event) 2)))))
      (event-store-append
       event-store
       "filter-edge-stream"
       (list (make-test-event "filter-edge-1" "filter-edge-stream" 1)))
      (expect (subscription-poll subscription) :to-be nil)
      (expect (subscription-cursor subscription) :to-be 1)
      (expect (subscription-offset offset-store "filter-edge") :to-be 1)
      (event-store-append
       event-store
       "filter-edge-stream"
       (list (make-test-event "filter-edge-2" "filter-edge-stream" 2))
       :expected-version 1)
      (signals error (deliver-subscription subscription 1))
      (let ((delivery (subscription-poll subscription)))
        (expect (subscription-delivery-p delivery) :to-be-truthy)
        (expect (subscription-delivery-after-global-position delivery)
                :to-be
                2))
      (signals error (subscription-ack subscription 1))
      (expect (subscription-ack subscription 2) :to-be 2)
      (expect (subscription-ack subscription 2) :to-be 2)
      (signals error (subscription-ack subscription 3))
      (multiple-value-bind (delivery count)
          (deliver-subscription subscription #'identity)
        (expect delivery :to-be nil)
        (expect count :to-be 0))))))

(defun edge-outbox-message
    (id &key (topic :edge-topic) (status :pending) (attempts 0) claimed-at
         (available-at 0) claim-token last-error dead-lettered-at)
  (make-outbox-message
   :id id
   :topic topic
   :payload (list :payload id)
   :metadata '(:source :edge)
   :created-at 0
   :status status
   :attempts attempts
   :claimed-at claimed-at
   :available-at available-at
   :claim-token claim-token
   :last-error last-error
   :dead-lettered-at dead-lettered-at))

(describe
 "durable outbox edge contracts"
 (it
  "validates message selection, leasing, acknowledgement, and retry states"
  (let ((message (edge-outbox-message "invalid-list")))
    (signals error (make-in-memory-outbox-store :lock 1))
    (signals error
      (make-in-memory-outbox-store :messages (cons message 1)))
    (signals error
      (make-in-memory-outbox-store :messages (list 1)))
    (signals outbox-message-conflict
      (make-in-memory-outbox-store
       :messages (list message (edge-outbox-message "invalid-list"))))
    (signals error
      (make-outbox-message :id "invalid-state"
                           :topic :edge
                           :dead-lettered-at :invalid)))
  (let* ((store
           (make-in-memory-outbox-store
            :messages
            (list (edge-outbox-message "pending-1")
                  (edge-outbox-message "pending-2")
                  (edge-outbox-message "future" :available-at 100)
                  (edge-outbox-message "available-now" :available-at nil)
                  (edge-outbox-message "in-flight"
                                       :status :in-flight
                                       :attempts 1
                                       :claimed-at 9)
                  (edge-outbox-message "delivered" :status :delivered))))
         (pending (outbox-read-pending store :now 10)))
    (expect (length pending) :to-be 3)
    (expect (length (outbox-read-pending store :now 10 :limit 1))
            :to-be
            1)
    (expect (length (outbox-read-all store)) :to-be 6)
    (expect (length (outbox-read-all store :status :pending)) :to-be 4)
    (expect (length (outbox-read-all store :status :pending :limit 1))
            :to-be
            1)
    (expect (outbox-read store "missing") :to-be nil)
    (signals error (outbox-read-pending store :now :invalid))
    (signals error (outbox-read-pending store :limit 0))
    (signals error
      (outbox-read-pending store :limit "not-an-integer"))
    (signals error (outbox-read-all store :status :invalid))
    (signals error (outbox-read-all store :limit 0))
    (signals error (outbox-claim store :now 10 :lease-seconds -1))
    (signals error
      (outbox-claim store :now 10 :lease-seconds "not-a-number"))
    (signals error (outbox-claim store :now :invalid))
    (expect (cl-event-sourcing-kit::%outbox-available-p
             (edge-outbox-message "invalid-availability"
                                  :available-at "not-a-time")
             10)
            :to-be
            nil)
    (let ((claimed (outbox-claim store :now 10 :limit 1 :lease-seconds 5)))
      (expect (length claimed) :to-be 1)
      (expect (outbox-message-status (first claimed)) :to-be :in-flight)
      (signals error
        (outbox-ack store
                    "pending-1"
                    :claim-token :stale))
      (expect (outbox-message-status
               (outbox-ack store
                           "pending-1"
                           :claim-token
                           (outbox-message-claim-token (first claimed))))
              :to-be
              :delivered)
      (expect (outbox-message-status
               (outbox-ack store "pending-1"))
              :to-be
              :delivered))
    (let ((unclaimed
            (make-in-memory-outbox-store
             :messages (list (edge-outbox-message "unclaimed-ack")))))
      (signals error
        (outbox-ack unclaimed "unclaimed-ack" :claim-token "unexpected"))
      (expect (outbox-message-status
               (outbox-ack unclaimed "unclaimed-ack"))
              :to-be
              :delivered)
      (expect (outbox-message-status
               (outbox-ack unclaimed "unclaimed-ack"))
              :to-be
              :delivered))
    (let ((not-expired
            (make-in-memory-outbox-store
             :messages (list (edge-outbox-message "not-expired"
                                                  :status :in-flight
                                                  :attempts 1
                                                  :claimed-at 9)))))
      (expect (outbox-claim not-expired :now 10 :lease-seconds 5)
              :to-be
              nil))
    (let ((future
            (make-in-memory-outbox-store
             :messages
             (list (edge-outbox-message "future-only" :available-at 100)))))
      (expect (outbox-claim future :now 10)
              :to-be
              nil))
    (let ((expired
            (make-in-memory-outbox-store
             :messages (list (edge-outbox-message "expired"
                                                  :status :in-flight
                                                  :attempts 1
                                                  :claimed-at 0)))))
      (let ((claimed (first (outbox-claim expired :now 10 :lease-seconds 5))))
        (expect (outbox-message-attempts claimed) :to-be 2)
        (expect (outbox-message-status claimed) :to-be :in-flight)))
    (let ((default-now
            (make-in-memory-outbox-store
             :messages (list (edge-outbox-message "default-now")))))
      (expect (length (outbox-claim default-now)) :to-be 1)))
  (let ((store
          (make-in-memory-outbox-store
           :messages (list (edge-outbox-message "retry")
                           (edge-outbox-message "retry-other")))))
    (expect (outbox-fail store "missing" :now 10) :to-be nil)
    (multiple-value-bind (failed dead-lettered-p)
        (outbox-fail store
                     "retry"
                     :now 10
                     :backoff-seconds 2
                     :failure 42)
      (expect (outbox-message-status failed) :to-be :pending)
      (expect (outbox-message-last-error failed) :to-equal "42")
      (expect (outbox-message-available-at failed) :to-be 12)
      (expect dead-lettered-p :to-be nil))
    (multiple-value-bind (failed dead-lettered-p)
        (outbox-fail store "retry" :now 12 :failure nil)
      (expect (outbox-message-last-error failed) :to-be nil)
      (expect dead-lettered-p :to-be nil)))
  (let ((preserved
          (make-in-memory-outbox-store
           :messages (list (edge-outbox-message "preserved"
                                                :last-error "old")))))
    (expect (outbox-message-last-error
             (outbox-fail preserved "preserved" :now 0))
            :to-equal
            "old"))
  (let* ((store (make-in-memory-outbox-store
                 :messages (list (edge-outbox-message "failure-token"))))
         (claimed (first (outbox-claim store :now 0)))
         (token (outbox-message-claim-token claimed)))
    (signals error
      (outbox-fail store "failure-token" :claim-token :stale :now 0))
    (outbox-fail store "failure-token" :claim-token token :now 0))
  (let ((dead
          (make-in-memory-outbox-store
           :messages (list (edge-outbox-message "dead"
                                                :status :dead-letter
                                                :attempts 2
                                                :last-error "failed"
                                                :dead-lettered-at 1)
                               (edge-outbox-message "dead-other")))))
    (expect (outbox-requeue dead "missing") :to-be nil)
    (signals error (outbox-ack dead "dead"))
    (signals error (outbox-fail dead "dead" :now 2))
    (let ((requeued (outbox-requeue dead "dead" :available-at 3)))
      (expect (outbox-message-status requeued) :to-be :pending)
      (expect (outbox-message-last-error requeued) :to-be nil)
      (expect (outbox-message-dead-lettered-at requeued) :to-be nil))
    (expect (outbox-message-status (outbox-requeue dead "dead"))
            :to-be
            :pending))
  (let ((pending (make-in-memory-outbox-store
                  :messages (list (edge-outbox-message "requeue-pending"))))
        (delivered (make-in-memory-outbox-store
                    :messages (list (edge-outbox-message "requeue-delivered"
                                                         :status :delivered)))))
    (expect (outbox-message-status (outbox-requeue pending "requeue-pending"))
            :to-be
            :pending)
    (signals error (outbox-requeue delivered "requeue-delivered")))
  (signals error (outbox-dispatch (make-in-memory-outbox-store) nil))
  (signals error
    (outbox-dispatch (make-in-memory-outbox-store)
                     #'identity
                     :max-attempts 0))
  (signals error
    (outbox-dispatch (make-in-memory-outbox-store)
                     #'identity
                     :max-attempts "not-an-integer"))
  (let ((store (make-in-memory-outbox-store
                :messages (list (edge-outbox-message "success")))))
    (multiple-value-bind (delivered claimed dead-lettered)
        (outbox-dispatch store
                         (lambda (message)
                           (expect (outbox-message-id message) :to-be "success"))
                         :now 0)
      (expect delivered :to-be 1)
      (expect claimed :to-be 1)
      (expect dead-lettered :to-be 0)
      (expect (outbox-message-status (outbox-read store "success"))
              :to-be
              :delivered)))))

(describe
 "durable file outbox edge contracts"
 (it
  "validates persistence, current records, and malformed wire data"
  (call-with-durable-test-path
   "outbox-file-edge"
   (lambda (path)
     (signals durable-store-corruption
       (cl-event-sourcing-kit::%outbox-messages-from-wire
        (cons :outbox-store 1)
        path))
     (signals durable-store-corruption
       (cl-event-sourcing-kit::%outbox-messages-from-wire
        '(:outbox-store :messages :invalid-list)
        path))
     (signals error
       (cl-event-sourcing-kit::%outbox-from-wire
        (cons :outbox-message 1)))
     (signals error (make-file-outbox-store :path path :sync 1))
     (signals error (make-file-outbox-store :path path :lock 1))
     (let* ((store (make-file-outbox-store :path path))
            (message (edge-outbox-message "file-1")))
       (expect (outbox-message-id (outbox-append store message))
               :to-equal
               "file-1")
       (expect (outbox-message-id (outbox-append store message))
               :to-equal
               "file-1")
       (signals outbox-message-conflict
         (outbox-append store
                        (edge-outbox-message "file-1"
                                              :topic :other-topic)))
       (expect (length (outbox-read-pending store :now 0 :limit 1))
               :to-be
               1)
       (expect (length (outbox-read-all store :status :pending :limit 1))
               :to-be
               1)
       (expect (outbox-read store "file-missing") :to-be nil)
       (expect (outbox-ack store "file-missing") :to-be nil)
       (expect (outbox-fail store "file-missing" :now 0) :to-be nil)
       (expect (outbox-requeue store "file-missing" :available-at 0)
               :to-be
               nil)
       (let ((claimed (first (outbox-claim store :now 0 :limit 1))))
         (expect (outbox-message-status
                  (outbox-ack store
                              "file-1"
                              :claim-token
                              (outbox-message-claim-token claimed)))
                 :to-be
                 :delivered)
          (expect (outbox-message-status (outbox-ack store "file-1"))
                  :to-be
                  :delivered))))))
 (it
  "keeps non-target file outbox records during failure and requeue"
  (call-with-durable-test-path
   "outbox-file-lifecycle-copy"
   (lambda (path)
     (let ((store (make-file-outbox-store :path path)))
       (outbox-append store (edge-outbox-message "file-fail-target"))
       (outbox-append store
                      (edge-outbox-message "file-requeue-target"
                                           :attempts 1))
       (outbox-append store (edge-outbox-message "file-other"))
       (multiple-value-bind (failed dead-lettered-p)
           (outbox-fail store "file-fail-target" :now 0)
         (expect (outbox-message-status failed) :to-be :pending)
         (expect dead-lettered-p :to-be nil))
       (multiple-value-bind (failed dead-lettered-p)
           (outbox-fail store
                        "file-requeue-target"
                        :now 0
                        :max-attempts 1)
         (expect (outbox-message-status failed) :to-be :dead-letter)
         (expect dead-lettered-p :to-be-truthy))
       (expect (outbox-message-status
                (outbox-requeue store
                                "file-requeue-target"
                                :available-at 4))
                :to-be
                :pending)
       (expect (outbox-message-status
                (outbox-read store "file-fail-target"))
                :to-be
                :pending)
       (expect (outbox-message-status
                (outbox-read store "file-other"))
                :to-be
                :pending)))))
  (call-with-durable-test-path
   "outbox-missing-current-field-edge"
   (lambda (path)
     (write-durable-edge-value
      path
      '(:outbox-store
        :messages
        ((:outbox-message
          :id "missing-fields"
          :topic :edge-topic
          :payload (:payload :missing-fields)
          :metadata (:source :edge)
          :created-at 0
          :status :pending
          :attempts 0
          :claimed-at nil
          :available-at 0
          :claim-token nil))))
     (signals durable-store-corruption
       (make-file-outbox-store :path path))))
  (call-with-durable-test-path
   "outbox-invalid-wire-edge"
   (lambda (path)
     (dolist (wire
               (list '(:invalid-envelope)
                     '(:outbox-store :messages :invalid-list)
                     '(:outbox-store :messages ((:invalid-record)))
                     '(:outbox-store
                       :messages
                       ((:outbox-message
                         :id "duplicate"
                         :topic :edge-topic
                         :payload (:payload :duplicate)
                         :metadata (:source :edge)
                         :created-at 0
                         :status :pending
                         :attempts 0
                         :claimed-at nil
                         :available-at 0
                         :claim-token nil)
                        (:outbox-message
                         :id "duplicate"
                         :topic :edge-topic
                         :payload (:payload :duplicate-2)
                         :metadata (:source :edge)
                         :created-at 0
                         :status :pending
                         :attempts 0
                         :claimed-at nil
                         :available-at 0
                         :claim-token nil)))))
       (write-durable-edge-value path wire)
       (signals durable-store-corruption
         (make-file-outbox-store :path path))))))

(describe
 "durable serialization edge contracts"
 (it
  "covers serializer validation, reader boundaries, and atomic cleanup"
  (let ((serializer (make-event-serializer)))
    (signals type-error
      (serialize-value :value :serializer :invalid))
    (signals type-error
      (cl-event-sourcing-kit::%safe-serializable-value-p
       '(:nested (:value))
       :max-depth 0))
    (expect
     (apply #'cl-event-sourcing-kit::%safe-serializable-value-p
            (list '(:nested (:value))))
     :to-be-truthy)
    (expect (cl-event-sourcing-kit::%safe-serializable-value-p
             '(:nested (:value))
             :max-depth 1)
            :to-be
            nil)
    (signals type-error
      (cl-event-sourcing-kit::%default-deserialize-value 1 serializer))
    (signals event-serialization-error
      (cl-event-sourcing-kit::%default-deserialize-value
       "#1=(a . #1#)"
       serializer))
    (signals event-serialization-error
      (deserialize-value 1))
    (signals event-serialization-error
      (deserialize-value
       "value"
       :serializer
       (make-event-serializer
        :decode
        (lambda (text)
          (declare (ignore text))
          (error 'event-serialization-error
                 :value "value"
                 :direction :decode
                 :cause "edge decoder")))))
    (signals event-serialization-error
      (serialize-value
       :value
       :serializer
       (make-event-serializer
        :encode
        (lambda (value)
          (declare (ignore value))
          (error 'event-serialization-error
                 :value :value
                 :direction :encode
                 :cause "edge encoder")))))
    (signals error (serialize-domain-event nil))
    (let* ((event (make-test-event "wire-invalid-version"
                                   "wire-stream"
                                   :payload))
           (wire (cl-event-sourcing-kit::%domain-event-wire event)))
      (setf (getf (rest wire) :version) -1)
      (signals event-sourcing-error
        (cl-event-sourcing-kit::%domain-event-from-wire wire)))
    (call-with-durable-test-path
     "serialization-atomic"
     (lambda (path)
       (cl-event-sourcing-kit::%write-serialized-file
        path
        '(:ready t)
        nil)
       (expect (cl-event-sourcing-kit::%read-serialized-file path nil)
               :to-equal
               '(:ready t))
       (signals type-error
         (cl-event-sourcing-kit::%write-serialized-file
          path
          '(:bad t)
          nil
          1))
       (signals event-serialization-error
         (cl-event-sourcing-kit::%write-serialized-file
          path
          '(:bad t)
          (make-event-serializer
           :encode
           (lambda (value)
             (declare (ignore value))
             (format nil "line1~%line2")))))
       (signals error
         (cl-event-sourcing-kit::%write-serialized-file
          path
          '(:bad t)
          nil
          (lambda (stream)
            (declare (ignore stream))
            (error "sync failed"))))
       (let* ((base (host-kit:temporary-directory))
              (target-name
                (format nil "cl-event-sourcing-kit-~A"
                        (gensym "RENAME-DIRECTORY-")))
              (target (merge-pathnames target-name base))
              (directory-path
                (merge-pathnames (format nil "~A/" target-name) base)))
       (unwind-protect
            (progn
              (ensure-directories-exist
               (merge-pathnames "marker" directory-path))
                (let* ((counter cl:*gensym-counter*)
                       (temporary
                         (let ((cl:*gensym-counter* counter))
                           (cl-event-sourcing-kit::%durable-temporary-pathname
                            target))))
                  (let ((cl:*gensym-counter* counter))
                    (signals error
                      (cl-event-sourcing-kit::%write-serialized-file
                       target
                       '(:bad t)
                       nil)))
                  (expect (probe-file temporary) :to-be nil)))
           (uiop:delete-directory-tree directory-path :validate t))))))))

 (it "removes a failed replacement journal"
   (call-with-durable-test-path
    "serialization-log-replace-cleanup"
    (lambda (path)
      (let ((store
              (make-file-event-store
               :path path
               :serializer
               (make-event-serializer
                :encode
                (lambda (value)
                  (when (equal value '(:bad))
                    (error "replacement serialization failure"))
                  (with-standard-io-syntax
                    (let ((*print-pretty* nil))
                      (write-to-string value :readably t))))))))
        (unwind-protect
               (let* ((counter cl:*gensym-counter*)
                      (temporary
                        (let ((cl:*gensym-counter* counter))
                          (cl-event-sourcing-kit::%durable-temporary-pathname
                           path))))
               (let ((cl:*gensym-counter* counter))
                 (let ((caught nil))
                   (handler-case
                       (cl-event-sourcing-kit::%replace-file-log-records
                        store
                        (list '(:ok) '(:bad)))
                     (error () (setf caught t)))
                   (expect caught :to-be t)))
               (expect (probe-file temporary) :to-be nil))
          (close-file-event-store store))))))

 (it "removes a replacement journal when renaming fails"
   (let* ((base (host-kit:temporary-directory))
          (target-name
            (format nil "cl-event-sourcing-kit-~A"
                    (gensym "RENAME-REPLACE-DIRECTORY-")))
          (target (merge-pathnames target-name base))
          (directory-path
            (merge-pathnames (format nil "~A/" target-name) base))
          (store
            (make-instance
             'cl-event-sourcing-kit::file-event-store
             :path target
             :serializer
             (make-event-serializer
              :encode
              (lambda (value)
                (with-standard-io-syntax
                  (let ((*print-pretty* nil))
                    (write-to-string value :readably t)))))
             :delegate nil
             :stream nil
             :sync #'finish-output
             :lock nil
             :global-position-start 0
             :next-transaction-id 0)))
     (unwind-protect
          (progn
            (ensure-directories-exist
             (merge-pathnames "marker" directory-path))
            (let* ((counter cl:*gensym-counter*)
                   (temporary
                     (let ((cl:*gensym-counter* counter))
                       (cl-event-sourcing-kit::%durable-temporary-pathname
                        target))))
              (let ((cl:*gensym-counter* counter))
                (let ((caught nil))
                  (handler-case
                      (cl-event-sourcing-kit::%replace-file-log-records
                       store
                       (list '(:ready)))
                    (error () (setf caught t)))
                  (expect caught :to-be t)))
              (expect (probe-file temporary) :to-be nil)))
       (uiop:delete-directory-tree directory-path :validate t))))

(describe
 "file event store edge contracts"
 (it
  "covers file operations, protocol methods, and transaction failure"
  (call-with-durable-test-path
   "file-event-store-edge"
   (lambda (path)
     (signals event-sourcing-error
       (make-file-event-store))
     (signals type-error
       (make-file-event-store :path path :sync 1))
     (signals type-error
       (make-file-event-store :path path :global-position-start -1))
     (signals type-error
       (make-file-event-store :path path
                              :global-position-start "not-an-integer"))
     (signals type-error
       (make-file-event-store :path path :lock 1))
     (let ((store (make-file-event-store :path path)))
       (unwind-protect
            (progn
              (signals type-error (file-event-store-sync nil))
              (signals type-error (close-file-event-store nil))
              (multiple-value-bind (events version)
                  (event-store-append
                   store
                   "file-edge-stream"
                   (list (make-test-event "file-edge-1"
                                          "file-edge-stream"
                                          :one)))
                (expect version :to-be 1)
                (expect (length events) :to-be 1)
                (expect (event-store-event-equivalent-p
                         store
                         (first events)
                         (first events))
                        :to-be-truthy))
              (event-store-append
               store
               "file-default-stream"
               (list (make-test-event "file-edge-2"
                                      "file-default-stream"
                                      :two)))
              (event-store-append-batch
               store
               (list
                (make-event-append-request
                 :stream-id "file-batch-stream"
                 :events
                 (list (make-test-event "file-edge-3"
                                        "file-batch-stream"
                                        :three))
                 :expected-version :no-stream)))
              (expect (length (event-store-read
                               store
                               "file-edge-stream"
                               :from-version 1
                               :to-version 1))
                      :to-be
                      1)
              (expect (length (event-store-read-all store :limit 2))
                      :to-be
                      2)
              (expect (event-store-current-version store "file-edge-stream")
                      :to-be
                      1)
              (expect (event-store-current-global-position store)
                      :to-be
                      3)
              (expect (event-store-stream-exists-p store "file-edge-stream")
                      :to-be-truthy)
              (expect (event-store-global-position-supported-p store)
                      :to-be-truthy)
              (expect (event-store-snapshots-supported-p store)
                      :to-be-truthy)
              (expect (event-store-retention-supported-p store)
                      :to-be-truthy)
              (expect (event-store-retention-floor store)
                      :to-be
                      0)
              (let ((snapshot (make-event-snapshot
                               :stream-id "file-edge-stream"
                               :version 1
                               :state '(:state :one)
                               :metadata '(:source :edge)
                               :timestamp 100)))
                (event-store-save-snapshot store snapshot)
                (let ((read-snapshot (event-store-read-snapshot
                                      store
                                      "file-edge-stream")))
                  (expect (event-snapshot-stream-id read-snapshot)
                          :to-equal
                          "file-edge-stream")
                  (expect (event-snapshot-version read-snapshot)
                          :to-be
                          1)
                  (expect (event-snapshot-state read-snapshot)
                          :to-equal
                          '(:state :one))
                  (expect (event-snapshot-metadata read-snapshot)
                          :to-equal
                          '(:source :edge))
                  (expect (event-snapshot-timestamp read-snapshot)
                          :to-be
                          100))
                (expect (event-store-delete-snapshot
                         store
                         "file-edge-stream")
                        :to-be
                        t)
                (expect (event-store-delete-snapshot
                         store
                         "file-edge-stream")
                        :to-be
                        nil)
                (expect (event-store-read-snapshot
                         store
                         "file-edge-stream")
                        :to-be
                        nil))
              (event-store-prune store)
              (file-event-store-sync store)
              (close-file-event-store store)
              (file-event-store-sync store)
              (signals error
                (event-store-append
                 store
                 "file-edge-stream"
                 (list (make-test-event "file-edge-conflict"
                                        "file-edge-stream"
                                        :bad))
                 :expected-version :no-stream)))
         (close-file-event-store store)
         (expect (close-file-event-store store) :to-be t)))))))

(describe
 "durable file recovery edge contracts"
 (it "rejects malformed journals and wraps replay failures"
   (flet ((expect-corruption (suffix records)
            (call-with-durable-test-path
             suffix
             (lambda (path)
               (write-durable-edge-readable-log path records)
               (signals durable-store-corruption
                 (make-file-event-store :path path))))))
     (expect-corruption "file-invalid-record"
                        (list "not-a-plist"))
     (expect-corruption "file-invalid-configuration"
                        (list (list :configuration
                                    :global-position-start -1)))
     (expect-corruption "file-malformed-configuration"
                        (list (list :configuration
                                    :global-position-start)))
     (expect-corruption "file-invalid-configuration-type"
                        (list (list :configuration
                                    :global-position-start
                                    "not-an-integer")))
     (expect-corruption "file-invalid-configuration-key"
                        (list (list :configuration
                                    :global-position-start
                                    0
                                    'cl-user::not-a-keyword
                                    1)))
     (expect-corruption "file-duplicate-configuration"
                        (list (list :configuration
                                    :global-position-start 0)
                              (list :configuration
                                    :global-position-start 0)))
     (expect-corruption "file-invalid-record-head"
                        (list (list 'cl-user::record :transaction-id 1)))
     (expect-corruption "file-invalid-record-tail"
                        (list (list :prepare
                                    :transaction-id 1
                                    'cl-user::not-a-keyword
                                    :value)))
     (expect-corruption "file-unknown-record"
                        (list '(:invalid-record)))
     (expect-corruption "file-unknown-record-with-properties"
                        (list '(:invalid-record :transaction-id 1)))
     (expect-corruption "file-invalid-transaction-id"
                        (list (list :prepare
                                    :transaction-id -1
                                    :operation
                                    (list :prune :before-global-position 0))))
     (expect-corruption "file-invalid-transaction-id-type"
                        (list (list :prepare
                                    :transaction-id "not-an-integer"
                                    :operation
                                    (list :prune :before-global-position 0))))
     (expect-corruption "file-invalid-transaction-record"
                        (list (cons :prepare 2)))
     (expect-corruption "file-missing-operation"
                        (list (list :prepare :transaction-id 1 :operation nil)))
     (expect-corruption "file-duplicate-prepare"
                        (list (list :prepare
                                    :transaction-id 1
                                    :operation
                                    (list :prune :before-global-position 0))
                              (list :prepare
                                    :transaction-id 1
                                    :operation
                                    (list :prune :before-global-position 0))))
     (expect-corruption "file-commit-without-prepare"
                        (list (list :commit :transaction-id 1)))
     (expect-corruption "file-duplicate-commit"
                        (list (list :prepare
                                    :transaction-id 1
                                    :operation
                                    (list :prune :before-global-position 0))
                              (list :commit :transaction-id 1)
                              (list :commit :transaction-id 1)))
     (expect-corruption "file-commit-abort-conflict"
                        (list (list :prepare
                                    :transaction-id 1
                                    :operation
                                    (list :prune :before-global-position 0))
                              (list :commit :transaction-id 1)
                              (list :abort :transaction-id 1)))
     (expect-corruption "file-abort-commit-conflict"
                        (list (list :prepare
                                    :transaction-id 1
                                    :operation
                                    (list :prune :before-global-position 0))
                              (list :abort :transaction-id 1)
                              (list :commit :transaction-id 1)))
     (expect-corruption "file-abort-without-prepare"
                        (list (list :abort :transaction-id 1)))
     (expect-corruption "file-duplicate-abort"
                        (list (list :prepare
                                    :transaction-id 1
                                    :operation
                                    (list :prune :before-global-position 0))
                              (list :abort :transaction-id 1)
                              (list :abort :transaction-id 1)))
     (expect-corruption "file-invalid-events"
                        (list (list :prepare
                                    :transaction-id 1
                                    :operation
                                    (list :append
                                          :stream-id "invalid-events"
                                          :events (cons 1 2)
                                          :expected-version :any))))
     (signals error
       (cl-event-sourcing-kit::%file-operation-events (cons 1 2)))
     (signals error
       (cl-event-sourcing-kit::%file-operation-requests
        (list (cons :request 1))))
     (expect-corruption "file-invalid-requests"
                        (list (list :prepare
                                    :transaction-id 1
                                    :operation
                                    (list :batch :requests (cons 1 2)))))
     (expect-corruption "file-invalid-request"
                        (list (list :prepare
                                    :transaction-id 1
                                    :operation
                                    (list :batch :requests (list '(:bad))))))
     (expect-corruption "file-invalid-operation"
                        (list (list :prepare
                                    :transaction-id 1
                                    :operation (cons :append 2))))
     (expect-corruption "file-unknown-operation"
                        (list (list :prepare
                                    :transaction-id 1
                                    :operation (list :unknown-operation))))
     (expect-corruption "file-replay-error"
                        (let ((event (make-test-event
                                      "file-replay-error"
                                      "file-replay-error"
                                      :payload)))
                          (list (list :prepare
                                      :transaction-id 1
                                      :operation
                                      (list :append
                                            :stream-id "file-replay-error"
                                            :events (list
                                                     (cl-event-sourcing-kit::%domain-event-wire
                                                      event))
                                            :expected-version 99))
                                (list :commit :transaction-id 1))))
     (call-with-durable-test-path
      "file-nested-corruption"
      (lambda (path)
        (let ((store (make-file-event-store :path path)))
          (unwind-protect
               (signals durable-store-corruption
                 (cl-event-sourcing-kit::%file-corruption
                  store
                  :nested
                  (make-condition 'durable-store-corruption
                                  :path path
                                  :record :inner
                                  :cause :inner)))
            (close-file-event-store store)))))))
  (it "preserves the application error when abort logging fails"
    (call-with-durable-test-path
     "file-abort-log-failure"
     (lambda (path)
       (let* ((sync-calls 0)
              (store (make-file-event-store
                      :path path
                      :sync (lambda (stream)
                              (incf sync-calls)
                              (if (= sync-calls 2)
                                  (error "abort log sync failure")
                                  (finish-output stream))))))
         (unwind-protect
              (signals error
                (cl-event-sourcing-kit::%file-transaction
                 store
                 :failing-operation
                 (lambda ()
                   (error "application failure"))))
           (close-file-event-store store))))))
  (it "swallows a failed standalone abort append"
    (call-with-durable-test-path
     "file-standalone-abort-log-failure"
     (lambda (path)
       (let* ((fail-p nil)
             (store
               (make-file-event-store
                :path path
                :sync (lambda (stream)
                        (if fail-p
                            (error "standalone abort log sync failure")
                            (finish-output stream))))))
         (unwind-protect
              (progn
                (setf fail-p t)
                (expect
                 (cl-event-sourcing-kit::%file-log-abort store 99)
                 :to-be
                 nil))
           (setf fail-p nil)
           (close-file-event-store store))))))
  (it "repairs a legacy journal with an incomplete tail"
    (call-with-durable-test-path
     "file-legacy-incomplete-tail"
     (lambda (path)
       (let ((event (make-test-event
                     "file-legacy-tail-event"
                     "file-legacy-tail-stream"
                     :payload)))
         (write-durable-edge-readable-log-with-tail
          path
          (list
           (list :prepare
                 :transaction-id 1
                 :operation
                 (list :append
                       :stream-id "file-legacy-tail-stream"
                       :events
                       (list (cl-event-sourcing-kit::%domain-event-wire
                              event))
                       :expected-version :no-stream))
           (list :commit :transaction-id 1))
          "(:incomplete")
         (let ((store (make-file-event-store :path path)))
           (unwind-protect
                (expect (mapcar #'domain-event-id
                                (event-store-read
                                 store
                                 "file-legacy-tail-stream"))
                        :to-equal
                        '("file-legacy-tail-event"))
             (close-file-event-store store)))
         (let ((store (make-file-event-store :path path)))
           (unwind-protect
                (expect (mapcar #'domain-event-id
                                (event-store-read-all store))
                        :to-equal
                        '("file-legacy-tail-event"))
             (close-file-event-store store)))))))
  (it "replays committed, uncommitted, snapshot, batch, delete, and prune operations"
    (call-with-durable-test-path
     "file-recovery-operations"
     (lambda (path)
       (let* ((append-event
                (make-test-event "file-recovery-append"
                                 "file-recovery-append"
                                 :append))
              (batch-event
                (make-test-event "file-recovery-batch"
                                 "file-recovery-batch"
                                 :batch))
              (uncommitted-event
                (make-test-event "file-recovery-uncommitted"
                                 "file-recovery-uncommitted"
                                 :uncommitted))
              (aborted-event
                (make-test-event "file-recovery-aborted"
                                 "file-recovery-aborted"
                                 :aborted))
              (snapshot
                (make-event-snapshot
                 :stream-id "file-recovery-snapshot"
                 :version 0
                 :state '(:state :saved)
                 :metadata '(:source :edge)
                 :timestamp 0))
              (records
                (list
                 (list :prepare
                       :transaction-id 1
                       :operation
                       (list :append
                             :stream-id "file-recovery-append"
                             :events (list
                                      (cl-event-sourcing-kit::%domain-event-wire
                                       append-event))
                             :expected-version :no-stream))
                 (list :commit :transaction-id 1)
                 (list :prepare
                       :transaction-id 2
                       :operation
                       (list :batch
                             :requests
                             (list
                              (list :request
                                    :stream-id "file-recovery-batch"
                                    :events (list
                                             (cl-event-sourcing-kit::%domain-event-wire
                                              batch-event))
                                    :expected-version :no-stream))))
                 (list :commit :transaction-id 2)
                 (list :prepare
                       :transaction-id 3
                       :operation
                       (list :snapshot
                             :snapshot
                             (cl-event-sourcing-kit::%snapshot-wire snapshot)))
                 (list :commit :transaction-id 3)
                 (list :prepare
                       :transaction-id 4
                       :operation
                       (list :delete-snapshot
                             :stream-id "file-recovery-missing-snapshot"))
                 (list :commit :transaction-id 4)
                 (list :prepare
                       :transaction-id 5
                       :operation
                       (list :prune :before-global-position 0))
                 (list :commit :transaction-id 5)
                 (list :prepare
                       :transaction-id 6
                       :operation
                       (list :append
                             :stream-id "file-recovery-uncommitted"
                             :events (list
                                      (cl-event-sourcing-kit::%domain-event-wire
                                       uncommitted-event))
                             :expected-version :no-stream))
                 (list :prepare
                       :transaction-id 7
                       :operation
                       (list :append
                             :stream-id "file-recovery-aborted"
                             :events (list
                                      (cl-event-sourcing-kit::%domain-event-wire
                                       aborted-event))
                             :expected-version :no-stream))
                 (list :abort :transaction-id 7))))
         (write-durable-edge-log path records)
         (let ((store (make-file-event-store :path path)))
           (unwind-protect
                (progn
                  (expect (length (event-store-read
                                   store
                                   "file-recovery-append"))
                          :to-be
                          1)
                  (expect (length (event-store-read
                                   store
                                   "file-recovery-batch"))
                          :to-be
                          1)
                  (expect (length (event-store-read
                                   store
                                   "file-recovery-uncommitted"))
                          :to-be
                          1)
                  (expect (event-store-read
                           store
                           "file-recovery-aborted")
                          :to-be
                          nil)
                  (expect (event-snapshot-state
                           (event-store-read-snapshot
                            store
                            "file-recovery-snapshot"))
                          :to-equal
                          '(:state :saved)))
             (close-file-event-store store)))))))
  (it "covers file transaction serialization and default arguments"
    (call-with-durable-test-path
     "file-newline-record"
     (lambda (path)
       (let ((store
               (make-file-event-store
                :path path
                :serializer
                (make-event-serializer
                 :encode (lambda (value)
                           (if (and (consp value)
                                    (eq (first value) :configuration))
                               (format nil "~S" value)
                               (format nil "line-one~%line-two")))))))
         (unwind-protect
              (signals event-serialization-error
                (event-store-append
                 store
                 "file-newline-record"
                 (list (make-test-event "file-newline-record"
                                        "file-newline-record"
                                        :payload))))
           (close-file-event-store store)))))
    (call-with-durable-test-path
     "file-return-record"
     (lambda (path)
       (let ((store
               (make-file-event-store
                :path path
                :serializer
                (make-event-serializer
                 :encode (lambda (value)
                           (if (and (consp value)
                                    (eq (first value) :configuration))
                               (format nil "~S" value)
                               (concatenate 'string
                                            "line-one"
                                            (string #\Return)
                                            "line-two")))))))
         (unwind-protect
              (signals event-serialization-error
                (event-store-append
                 store
                 "file-return-record"
                 (list (make-test-event "file-return-record"
                                        "file-return-record"
                                        :payload))))
           (close-file-event-store store)))))
    (call-with-durable-test-path
     "file-decode-record"
     (lambda (path)
       (write-durable-edge-lines path (list "(:prepare)"))
       (signals durable-store-corruption
         (make-file-event-store
          :path path
          :serializer
          (make-event-serializer
           :decode (lambda (value)
                     (declare (ignore value))
                     (error "decoder failure")))))))
    (signals type-error (recover-file-event-store nil))
    (signals event-sourcing-error
      (apply #'make-file-event-store nil))
    (call-with-durable-test-path
     "file-default-arguments"
     (lambda (path)
       (let ((store (apply #'make-file-event-store (list :path path))))
         (unwind-protect
              (progn
                (expect
                 (length
                  (apply #'event-store-append
                         (list store
                               "file-default-arguments"
                               (list (make-test-event
                                      "file-default-arguments"
                                      "file-default-arguments"
                                      :payload)))))
                 :to-be
                 1)
                (signals type-error
                  (event-store-append-batch store (list :invalid-request)))
                (expect (length
                         (event-store-read store "file-default-arguments"))
                        :to-be
                        1))
           (close-file-event-store store)))))))
