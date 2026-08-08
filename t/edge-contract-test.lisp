(in-package #:cl-event-sourcing-kit/test)

(defun exercise-event-store-contract (constructor)
  (let* ((store (funcall constructor))
         (stream-id "contract-stream")
         (first-event (make-test-event "contract-1" stream-id :first))
         (second-event (make-test-event "contract-2" stream-id :second)))
    (expect (typep store 'event-store) :to-be-truthy)
    (multiple-value-bind (committed version) (event-store-append
                                              store
                                              stream-id
                                              (list first-event)
                                              :expected-version
                                              :no-stream)
      (expect (mapcar #'domain-event-id committed) :to-equal '("contract-1"))
      (expect version :to-be 1)
      (expect (domain-event-version (first committed)) :to-be 1)
      (expect (domain-event-global-position (first committed)) :to-be-truthy)
      (multiple-value-bind (retry retry-version) (event-store-append
                                                  store
                                                  stream-id
                                                  (list first-event)
                                                  :expected-version
                                                  0)
        (expect (first retry) :to-be (first committed))
        (expect retry-version :to-be 1)))
    (multiple-value-bind (committed version) (event-store-append
                                              store
                                              stream-id
                                              (list second-event)
                                              :expected-version
                                              1)
      (expect (mapcar #'domain-event-id committed) :to-equal '("contract-2"))
      (expect version :to-be 2))
    (let ((all (event-store-read-all store)))
      (expect
       (mapcar #'domain-event-id all)
       :to-equal
       '("contract-1" "contract-2"))
      (expect
       (event-store-read-all
        store
        :after-global-position
        (domain-event-global-position (first all)))
       :to-equal
       (list (second all)))
      (expect
       (event-store-read-all store :limit 1)
       :to-equal
       (list (first all)))
      (expect
       (event-store-current-global-position store)
       :to-be
       (domain-event-global-position (second all)))
      (expect
       (event-store-read store stream-id :from-version 2)
       :to-equal
       (list (second all)))
      (expect
       (event-store-event-equivalent-p store first-event (first all))
       :to-be-truthy))
    (expect (event-store-current-version store stream-id) :to-be 2)
    (expect (event-store-current-version store "missing-stream") :to-be 0)
    (expect (event-store-stream-exists-p store stream-id) :to-be-truthy)
    (expect (event-store-stream-exists-p store "missing-stream") :to-be nil)
    (values)))

(defmacro define-event-store-contract (description constructor)
  `(it ,description (exercise-event-store-contract ,constructor)))

(defclass malformed-global-position-store (event-store)
  ())

(defmethod event-store-global-position-supported-p ((store
                                                     malformed-global-position-store))
  (declare (ignore store))
  t)

(defmethod event-store-read-all ((store malformed-global-position-store)
                                 &key
                                 after-global-position
                                 limit)
  (declare (ignore store after-global-position limit))
  (list (make-test-event "malformed-1" "malformed-stream" :payload)))

(describe
 "abstract event-store contract"
 (define-event-store-contract
  "validates the in-memory store contract"
  #'make-event-store)
 (define-event-store-contract
  "validates the contract with an injected global position origin"
  (lambda ()
    (make-in-memory-event-store :global-position-start 10))))

(describe
 "boundary and adapter contracts"
 (it
  "generates events with defaults and rejects broken injected sources"
  (let ((event (make-domain-event :type :defaulted :stream-id "default-stream")))
    (expect (domain-event-id event) :to-be-truthy)
    (expect (domain-event-timestamp event) :to-be-truthy)
    (expect (domain-event-aggregate-id event) :to-be "default-stream")
    (expect (domain-event-payload event) :to-be nil)
    (expect (domain-event-metadata event) :to-be nil)
    (expect (domain-event-version event) :to-be nil)
    (expect (domain-event-global-position event) :to-be nil)
    (expect (stringp (princ-to-string event)) :to-be-truthy))
  (signals invalid-domain-event (make-domain-event :stream-id "missing-type"))
  (signals
   invalid-domain-event
   (make-domain-event :type :event :stream-id "stream-1" :aggregate-id nil))
  (signals
   invalid-domain-event
   (make-domain-event :type :event :stream-id "stream-1" :version :invalid))
  (signals
   invalid-domain-event
   (make-domain-event
    :type
    :event
    :stream-id
    "stream-1"
    :id-source
    (lambda ()
      nil)))
  (signals
   invalid-domain-event
   (make-domain-event
    :type
    :event
    :stream-id
    "stream-1"
    :id-source
    (make-hash-table)))
  (signals
   invalid-domain-event
   (make-domain-event
    :type
    :event
    :stream-id
    "stream-1"
    :clock
    (lambda ()
      (error "injected clock failure"))))
  (signals
   invalid-domain-event
   (make-domain-event
    :type
    :event
    :stream-id
    "stream-1"
    :clock
    (make-hash-table))))
 (it
  "keeps the reference store atomic at input boundaries"
  (let ((store (make-in-memory-event-store)))
    (expect (in-memory-event-store-p store) :to-be-truthy)
    (expect (in-memory-event-store-p nil) :to-be nil)
    (expect (event-store-current-global-position store) :to-be 0)
    (signals type-error (make-in-memory-event-store :global-position-start -1))
    (signals
     type-error
     (make-in-memory-event-store :global-position-start :invalid))
    (signals type-error (make-in-memory-event-store :lock nil))
    (signals invalid-domain-event (event-store-append store nil nil))
    (signals
     invalid-expected-version
     (event-store-append store "invalid-version" nil :expected-version -1))
    (multiple-value-bind (events version) (event-store-append
                                           store
                                           "empty-stream"
                                           nil
                                           :expected-version
                                           :no-stream)
      (expect events :to-be nil)
      (expect version :to-be 0))
    (expect (event-store-stream-exists-p store "empty-stream") :to-be nil)
    (let ((event (make-test-event "valid-1" "stream-1" :payload)))
      (signals
       invalid-domain-event
       (event-store-append store "stream-1" :not-events))
      (signals
       invalid-domain-event
       (event-store-append store "stream-1" (cons event :tail)))
      (signals
       invalid-domain-event
       (event-store-append
        store
        "stream-1"
        (list (make-test-event "mismatch" "other-stream" :payload))))
      (signals
       invalid-domain-event
       (event-store-append store "stream-1" (list :not-an-event)))
      (signals
       invalid-domain-event
       (event-store-append
        store
        "stream-1"
        (list
         (make-test-event "assigned-version" "stream-1" :payload :version 0))))
      (signals
       invalid-domain-event
       (event-store-append
        store
        "stream-1"
        (list
         (make-test-event
          "assigned-position"
          "stream-1"
          :payload
          :global-position
          0))))
      (event-store-append
       store
       "stream-1"
       (list event)
       :expected-version
       :no-stream)
      (multiple-value-bind (implicit-events implicit-version) (event-store-append
                                                               store
                                                               "implicit-version-stream"
                                                               (list
                                                                (make-test-event
                                                                 "implicit-version"
                                                                 "implicit-version-stream"
                                                                 :payload)))
        (expect (length implicit-events) :to-be 1)
        (expect implicit-version :to-be 1))
      (signals
       event-version-conflict
       (event-store-append store "stream-1" nil :expected-version :no-stream)))
    (signals type-error (event-store-read store "stream-1" :from-version -1))
    (signals
     type-error
     (event-store-read store "stream-1" :from-version :invalid))
    (signals type-error (event-store-read store "stream-1" :to-version -1))
    (signals type-error (event-store-read-all store :after-global-position -1))
    (signals type-error (event-store-read-all store :limit -1))
    (signals type-error (event-store-read-all store :limit :invalid))
    (expect
     (event-store-read-all store :after-global-position nil :limit 0)
     :to-be
     nil)))
 (it
  "exposes every unsupported adapter operation as a structured condition"
  (let* ((store (make-instance 'unsupported-store))
         (event (make-test-event "adapter-1" "adapter-stream" :payload)))
    (signals
     event-store-operation-not-supported
     (event-store-append store "adapter-stream" nil))
    (signals
     event-store-operation-not-supported
     (event-store-read store "adapter-stream"))
    (signals event-store-operation-not-supported (event-store-read-all store))
    (signals
     event-store-operation-not-supported
     (event-store-current-version store "adapter-stream"))
    (signals
     event-store-operation-not-supported
     (event-store-current-global-position store))
    (signals
     event-store-operation-not-supported
     (event-store-stream-exists-p store "adapter-stream"))
    (expect (event-store-global-position-supported-p store) :to-be nil)
    (expect (event-store-event-equivalent-p store event event) :to-be-truthy)
    (expect
     (event-store-event-equivalent-p store :not-an-event event)
     :to-be
     nil)
    (expect
     (event-store-event-equivalent-p store event :not-an-event)
     :to-be
     nil)))
 (it
  "compares every event identity field before accepting a duplicate"
  (let ((store (make-instance 'unsupported-store)))
    (flet ((make-equivalent-event (&key
                                   (id "eq-id")
                                   (type :eq-type)
                                   (stream-id "eq-stream")
                                   (aggregate-id "eq-aggregate")
                                   (payload :eq-payload)
                                   (metadata :eq-metadata)
                                   (timestamp 1)
                                   (correlation-id "eq-correlation")
                                   (causation-id "eq-causation"))
             (make-domain-event
              :id
              id
              :type
              type
              :stream-id
              stream-id
              :aggregate-id
              aggregate-id
              :payload
              payload
              :metadata
              metadata
              :timestamp
              timestamp
              :correlation-id
              correlation-id
              :causation-id
              causation-id)))
      (let* ((base (make-equivalent-event))
             (variants
              (list
               (make-equivalent-event :id "other-id")
               (make-equivalent-event :type :other-type)
               (make-equivalent-event :stream-id "other-stream")
               (make-equivalent-event :aggregate-id "other-aggregate")
               (make-equivalent-event :payload :other-payload)
               (make-equivalent-event :metadata :other-metadata)
               (make-equivalent-event :timestamp 2)
               (make-equivalent-event :correlation-id "other-correlation")
               (make-equivalent-event :causation-id "other-causation"))))
        (expect (event-store-event-equivalent-p store base base) :to-be-truthy)
        (dolist (variant variants)
          (expect
           (event-store-event-equivalent-p store base variant)
           :to-be
           nil))))))
 (it
  "keeps replay and staging input contracts explicit"
  (let ((event (make-test-event "replay-1" "replay-stream" 1))
        (staging (make-event-staging "replay-stream")))
    (expect (event-staging-p staging) :to-be-truthy)
    (expect (event-staging-p nil) :to-be nil)
    (expect
     (replay-events
      0
      #()
      (lambda (state current)
        (declare (ignore current))
        state))
     :to-be
     0)
    (signals type-error (replay-events 0 (list event) nil))
    (signals
     invalid-domain-event
     (replay-events 0 (list :not-an-event) #'identity))
    (signals invalid-domain-event (make-event-staging nil))
    (signals
     invalid-expected-version
     (make-event-staging "replay-stream" :expected-version -1))))
 (it
  "supports an explicit projection state and rejects invalid rebuild inputs"
  (let ((projection
         (make-projection
          :state
          :explicit-state
          :handler
          (lambda (state event)
            (declare (ignore event))
            state))))
    (expect (projection-p projection) :to-be-truthy)
    (expect (projection-p nil) :to-be nil)
    (expect (projection-state projection) :to-be :explicit-state)
    (signals type-error (make-projection :name "missing-handler"))
    (signals
     type-error
     (make-projection
      :handler
      (lambda (state event)
        (declare (ignore event))
        state)
      :checkpoint
      :invalid))
    (signals type-error (rebuild-projection nil (make-event-store)))))
 (it
  "reports malformed global positions without advancing a projection"
  (let* ((projection
          (make-projection
           :initial-state
           :initial
           :handler
           (lambda (state event)
             (declare (ignore event))
             state)))
         (condition nil))
    (handler-case (rebuild-projection
                   projection
                   (make-instance 'malformed-global-position-store))
      (projection-failure (caught)
        (setf condition caught)))
    (expect (typep condition 'projection-failure) :to-be-truthy)
    (expect (projection-failure-global-position condition) :to-be nil)
    (expect (projection-failure-checkpoint condition) :to-be 0)
    (expect
     (typep (projection-failure-cause condition) 'type-error)
     :to-be-truthy)
    (expect (projection-state projection) :to-be :initial)
    (expect (projection-checkpoint projection) :to-be 0)))
 (it
  "supports default and explicit continuation paths"
  (let ((store (make-event-store))
        (append-version nil)
        (replay-condition nil)
        (commit-version nil))
    (event-store-append/cc
     store
     "cc-empty"
     nil
     (lambda (events version)
       (expect events :to-be nil)
       (setf append-version version)))
    (expect append-version :to-be 0)
    (signals
     type-error
     (event-store-append/cc
      store
      "cc-empty"
      nil
      (lambda (&rest values)
        (declare (ignore values)))
      :on-error
      nil))
    (signals
     type-error
     (event-store-read/cc
      store
      "cc-empty"
      (lambda (&rest values)
        (declare (ignore values)))
      :on-error
      nil))
    (replay-events/cc
     0
     (list :not-an-event)
     #'identity
     (lambda (&rest values)
       (declare (ignore values)))
     :on-error
     (lambda (condition)
       (setf replay-condition condition)))
    (expect (typep replay-condition 'invalid-domain-event) :to-be-truthy)
    (let ((staging
           (make-event-staging "cc-stream" :expected-version :no-stream)))
      (stage-event staging (make-test-event "cc-1" "cc-stream" :payload))
      (commit-events/cc
       staging
       store
       (lambda (events version)
         (declare (ignore events))
         (setf commit-version version))
       :expected-version
       :no-stream)
      (expect commit-version :to-be 1)
      (expect (uncommitted-events staging) :to-be nil)
      (let ((projection
             (make-projection
              :handler
              (lambda (state event)
                (declare (ignore event))
                state))))
        (rebuild-projection/cc
         projection
         store
         (lambda (state checkpoint)
           (expect state :to-be nil)
           (expect checkpoint :to-be 1))))))))
