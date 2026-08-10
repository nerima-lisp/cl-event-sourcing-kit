(in-package #:cl-event-sourcing-kit/test)

(defclass invalid-snapshot-result-store (event-store)
  ())

(defmethod event-store-snapshots-supported-p
    ((store invalid-snapshot-result-store))
  (declare (ignore store))
  t)

(defmethod event-store-read-snapshot
    ((store invalid-snapshot-result-store) stream-id &key version)
  (declare (ignore store stream-id version))
  :not-a-snapshot)

(defclass mismatched-snapshot-store (event-store)
  ())

(defmethod event-store-snapshots-supported-p
    ((store mismatched-snapshot-store))
  (declare (ignore store))
  t)

(defmethod event-store-read-snapshot
    ((store mismatched-snapshot-store) stream-id &key version)
  (declare (ignore store stream-id version))
  (make-event-snapshot
   :stream-id
   "other-stream"
   :version
   1
   :state
   0))

(defclass invalid-snapshot-version-store (event-store)
  ())

(defmethod event-store-snapshots-supported-p
    ((store invalid-snapshot-version-store))
  (declare (ignore store))
  t)

(defmethod event-store-read-snapshot
    ((store invalid-snapshot-version-store) stream-id &key version)
  (declare (ignore store stream-id version))
  (make-instance
   'event-snapshot
   :stream-id
   "schema-stream"
   :version
   :invalid
   :state
   0))

(defclass invalid-replay-event-store (event-store)
  ())

(defmethod event-store-read
    ((store invalid-replay-event-store) stream-id &key from-version to-version)
  (declare (ignore store stream-id from-version to-version))
  (list :not-an-event))

(defclass mismatched-replay-event-store (event-store)
  ())

(defmethod event-store-read
    ((store mismatched-replay-event-store) stream-id &key from-version to-version)
  (declare (ignore store stream-id from-version to-version))
  (list (make-test-event "wrong-stream-event" "other-stream" :wrong)))

(defclass non-advancing-replay-event-store (event-store)
  ())

(defmethod event-store-read
    ((store non-advancing-replay-event-store) stream-id &key from-version to-version)
  (declare (ignore store from-version to-version))
  (list
   (make-test-event
    "version-0"
    stream-id
    :version-0
    :version
    0)))

(defclass unassigned-replay-event-store (event-store)
  ())

(defmethod event-store-read
    ((store unassigned-replay-event-store) stream-id &key from-version to-version)
  (declare (ignore store from-version to-version))
  (list
   (make-test-event
    "unassigned-version"
    stream-id
    :unassigned
    :version
    nil)))

(describe
 "replay and uncommitted staging"
 (it
  "folds events left to right as a pure operation"
  (let ((events
         (list
          (make-test-event "event-1" "stream-1" 2)
          (make-test-event "event-2" "stream-1" 3))))
    (expect
     (replay-events
      10
      events
      (lambda (state event)
        (+ state (domain-event-payload event))))
     :to-be
     15)
    (expect
     (replay-events
      nil
      events
      (lambda (state event)
        (declare (ignore event))
        state))
     :to-be
     nil)))
 (it
  "stages one stream and clears only after a successful commit"
  (let* ((store (make-event-store))
         (first-event (make-test-event "event-1" "stream-1" :first))
         (second-event (make-test-event "event-2" "stream-1" :second)))
    (with-event-staging
     (staging "stream-1")
     (stage-event staging first-event)
     (stage-event staging second-event)
     (expect
      (uncommitted-events staging)
      :to-equal
      (list first-event second-event))
     (multiple-value-bind (committed version) (commit-events staging store)
       (expect (length committed) :to-be 2)
       (expect version :to-be 2))
     (expect (uncommitted-events staging) :to-be nil)
     (expect (event-staging-expected-version staging) :to-be 2))))
 (it
  "retains staged events after an optimistic conflict"
  (let* ((store (make-event-store))
         (staging (make-event-staging "stream-1" :expected-version 0))
         (staged (make-test-event "staged" "stream-1" :staged))
         (other (make-test-event "other" "stream-1" :other)))
    (stage-event staging staged)
    (event-store-append
     store
     "stream-1"
     (list other)
     :expected-version
     :no-stream)
    (signals event-version-conflict (commit-events staging store))
    (expect (uncommitted-events staging) :to-equal (list staged))
    (expect (event-staging-expected-version staging) :to-be 0)
    (commit-events staging store :expected-version 1)
    (expect (uncommitted-events staging) :to-be nil)))
 (it
  "rejects events from another stream or with assigned positions"
  (let ((staging (make-event-staging "stream-1")))
    (signals
     invalid-domain-event
     (stage-event staging (make-test-event "event-1" "stream-2" :wrong)))
    (signals
     invalid-domain-event
     (stage-event
      staging
      (make-test-event "event-2" "stream-1" :version-0 :version 0)))
    (signals
     invalid-domain-event
     (stage-event
      staging
      (make-test-event "event-3" "stream-1" :global-0 :global-position 0)))))
 (it
  "rejects non-event input and invalid reducer arguments"
  (signals
   invalid-domain-event
   (stage-event (make-event-staging "stream-1") :not-an-event))
  (signals type-error (replay-events nil nil nil)))
 (it
  "validates staging construction and supports explicit append expectations"
  (signals invalid-domain-event (make-event-staging nil))
  (signals
   invalid-expected-version
   (make-event-staging "stream-1" :expected-version :invalid))
  (let ((store (make-event-store))
        (staging (make-event-staging "stream-1" :expected-version :any)))
    (stage-event staging (make-test-event "event-1" "stream-1" 1))
    (commit-events staging store :expected-version :no-stream)
    (expect (event-staging-expected-version staging) :to-be 1))))

(describe
 "replay schema evolution and aggregate loading"
 (it
  "upcasts stored events and starts aggregate replay from a snapshot"
  (let* ((store (make-event-store))
         (first-event (make-test-event
                       "schema-1"
                       "schema-stream"
                       1
                       :schema-version
                       1))
         (second-event (make-test-event
                        "schema-2"
                        "schema-stream"
                        2
                        :schema-version
                        1)))
    (event-store-append
     store
     "schema-stream"
     (list first-event second-event)
     :expected-version
     :no-stream)
    (event-store-save-snapshot
     store
     (make-event-snapshot
      :stream-id
      "schema-stream"
      :version
      1
      :state
      10))
    (flet ((upgrade (event)
             (if (= (domain-event-schema-version event) 1)
                 (make-domain-event
                  :id
                  (domain-event-id event)
                  :type
                  (domain-event-type event)
                  :stream-id
                  (domain-event-stream-id event)
                  :aggregate-id
                  (domain-event-aggregate-id event)
                  :payload
                  (* 10 (domain-event-payload event))
                  :metadata
                  (domain-event-metadata event)
                  :timestamp
                  (domain-event-timestamp event)
                  :schema-version
                  2
                  :version
                  (domain-event-version event)
                  :correlation-id
                  (domain-event-correlation-id event)
                  :causation-id
                  (domain-event-causation-id event)
                  :global-position
                  (domain-event-global-position event))
               event)))
      (multiple-value-bind (state version)
          (load-aggregate
           store
           "schema-stream"
           0
           (lambda (current event)
             (+ current (domain-event-payload event)))
           :upcaster
           #'upgrade)
        (expect state :to-be 30)
        (expect version :to-be 2))
      (multiple-value-bind (state version)
          (load-aggregate
           store
           "schema-stream"
           0
           (lambda (current event)
             (+ current (domain-event-payload event)))
           :use-snapshot
           nil
           :upcaster
           #'upgrade)
        (expect state :to-be 30)
        (expect version :to-be 2))
      (expect
       (replay-events
        0
        (vector first-event second-event)
        (lambda (current event)
          (+ current (domain-event-payload event)))
        :upcaster
        #'upgrade)
       :to-be
       30)
      (expect (upcast-event first-event nil) :to-be first-event))))
 (it
  "validates upcaster contracts and snapshot options"
  (let ((event (make-test-event "schema-1" "schema-stream" 1))
        (store (make-event-store)))
    (signals type-error (upcast-event event :not-a-function))
    (signals
     invalid-domain-event
     (upcast-event event (lambda (current) (declare (ignore current)) :not-event)))
    (signals
     type-error
     (load-aggregate
      store
      "schema-stream"
      0
      #'identity
      :use-snapshot
      :yes))
    (signals
     invalid-snapshot
     (load-aggregate
      (make-instance 'invalid-snapshot-result-store)
      "schema-stream"
      0
      #'identity))
    (signals
     invalid-snapshot
     (load-aggregate
      (make-instance 'mismatched-snapshot-store)
      "schema-stream"
      0
      #'identity))
    (signals
     invalid-snapshot
     (load-aggregate
      (make-instance 'invalid-snapshot-version-store)
      "schema-stream"
      0
      #'identity))
    (signals
     invalid-domain-event
     (load-aggregate
      (make-instance 'invalid-replay-event-store)
      "schema-stream"
      0
      #'identity
      :use-snapshot
      nil))
    (signals
     invalid-domain-event
     (load-aggregate
      (make-instance 'non-advancing-replay-event-store)
      "schema-stream"
      0
      (lambda (current event)
        (declare (ignore event))
        current)
      :use-snapshot
      nil))
    (signals
     invalid-domain-event
     (upcast-events (list event :not-event) nil))
    (let ((caught nil))
      (handler-case
          (load-aggregate
           store
           "schema-stream"
           0
           #'identity
           :use-snapshot
           :yes)
        (type-error () (setf caught t)))
      (expect caught :to-be t))
    (let ((caught nil))
      (handler-case
          (load-aggregate
           (make-instance 'invalid-snapshot-result-store)
           "schema-stream"
           0
           #'identity)
        (invalid-snapshot () (setf caught t)))
      (expect caught :to-be t))
    (let ((caught nil))
      (handler-case
          (load-aggregate
           (make-instance 'mismatched-snapshot-store)
           "schema-stream"
           0
           #'identity)
        (invalid-snapshot () (setf caught t)))
      (expect caught :to-be t))
    (let ((caught nil))
      (handler-case
          (load-aggregate
           (make-instance 'invalid-replay-event-store)
           "schema-stream"
           0
           #'identity
           :use-snapshot
           nil)
        (invalid-domain-event () (setf caught t)))
      (expect caught :to-be t))
    (let ((caught nil))
      (handler-case
          (load-aggregate
           (make-instance 'unassigned-replay-event-store)
           "schema-stream"
           0
           #'identity
           :use-snapshot
           nil)
        (invalid-domain-event () (setf caught t)))
      (expect caught :to-be t))
    (let ((caught nil))
      (handler-case
          (load-aggregate
           (make-instance 'mismatched-replay-event-store)
           "schema-stream"
           0
           #'identity
           :use-snapshot
           nil)
        (invalid-domain-event () (setf caught t)))
      (expect caught :to-be t))
    (event-store-append
     store
     "schema-stream"
     (list event)
     :expected-version
     :no-stream)
    (let ((caught nil))
      (handler-case
          (load-aggregate
           store
           "schema-stream"
           0
           #'identity
           :use-snapshot
           nil
           :upcaster
           (lambda (stored-event)
             (make-test-event
              "wrong-upcast-event"
              "other-stream"
              (domain-event-payload stored-event)
              :version
              (domain-event-version stored-event))))
        (invalid-domain-event () (setf caught t)))
      (expect caught :to-be t)))))
