(in-package #:cl-event-sourcing-kit/test)

(defclass projection-read-failure-store ()
  ())

(defmethod event-store-global-position-supported-p
    ((store projection-read-failure-store))
  (declare (ignore store))
  t)

(defmethod event-store-read-all
    ((store projection-read-failure-store) &key after-global-position limit)
  (declare (ignore store after-global-position limit))
  (error "synthetic projection read failure"))

(defclass malformed-projection-event-store ()
  ())

(defmethod event-store-global-position-supported-p
    ((store malformed-projection-event-store))
  (declare (ignore store))
  t)

(defmethod event-store-read-all
    ((store malformed-projection-event-store) &key after-global-position limit)
  (declare (ignore store after-global-position limit))
  (list :not-an-event))

(defclass nonmonotonic-global-position-store ()
  ())

(defmethod event-store-global-position-supported-p
    ((store nonmonotonic-global-position-store))
  (declare (ignore store))
  t)

(defmethod event-store-read-all
    ((store nonmonotonic-global-position-store) &key after-global-position limit)
  (declare (ignore store after-global-position limit))
  (list
   (make-test-event
    "nonmonotonic-1"
    "projection-stream"
    1
    :version
    1
    :global-position
    1)
   (make-test-event
    "nonmonotonic-2"
    "projection-stream"
    2
    :version
    2
    :global-position
    1)))

(describe
 "projection rebuild"
 (it
  "folds the global event stream and advances its checkpoint"
  (let ((store (make-event-store)))
    (event-store-append
     store
     "stream-1"
     (list
      (make-test-event "event-1" "stream-1" 1)
      (make-test-event "event-2" "stream-1" 2)
      (make-test-event "event-3" "stream-1" 3))
     :expected-version
     :no-stream)
    (let ((projection
           (make-projection
            :name
            :sum
            :initial-state
            0
            :handler
            (lambda (state event)
              (+ state (domain-event-payload event))))))
      (multiple-value-bind (state checkpoint) (rebuild-projection
                                               projection
                                               store)
        (expect state :to-be 6)
        (expect checkpoint :to-be 3)
        (expect (projection-state projection) :to-be 6)
        (expect (projection-checkpoint projection) :to-be 3))
      (multiple-value-bind (state checkpoint) (rebuild-projection
                                               projection
                                               store
                                               :from-global-position
                                               1)
        (expect state :to-be 5)
        (expect checkpoint :to-be 3)))))
 (it
  "uses a fresh initial state factory for mutable projections"
  (let ((store (make-event-store)))
    (event-store-append
     store
     "stream-1"
     (list (make-test-event "event-1" "stream-1" 1))
     :expected-version
     :no-stream)
    (let ((projection
           (make-projection
            :initial-state-factory
            (lambda ()
              (list 0))
            :handler
            (lambda (state event)
              (list (+ (first state) (domain-event-payload event)))))))
      (rebuild-projection projection store)
      (expect (projection-state projection) :to-equal '(1))
      (let ((first-state (projection-state projection)))
        (rebuild-projection projection store)
        (expect (projection-state projection) :to-equal '(1))
        (expect (eq first-state (projection-state projection)) :to-be nil)))))
 (it
  "reports the failed event and keeps the last successful checkpoint"
  (let ((store (make-event-store)))
    (event-store-append
     store
     "stream-1"
     (list
      (make-test-event "event-1" "stream-1" 1)
      (make-test-event "event-2" "stream-1" 2)
      (make-test-event "event-3" "stream-1" 3))
     :expected-version
     :no-stream)
    (let* ((projection
            (make-projection
             :name
             :failing
             :initial-state
             0
             :handler
             (lambda (state event)
               (if (= 2 (domain-event-payload event)) (error
                                                       "projection stopped")
                 (+ state (domain-event-payload event))))))
           (condition nil))
      (handler-case (rebuild-projection projection store)
        (projection-failure (caught)
          (setf condition caught)))
      (expect condition :to-be-truthy)
      (expect (projection-failure-projection condition) :to-be projection)
      (expect
       (domain-event-id (projection-failure-event condition))
       :to-equal
       "event-2")
      (expect (projection-failure-global-position condition) :to-be 2)
      (expect (projection-failure-checkpoint condition) :to-be 1)
      (expect (typep (projection-failure-cause condition) 'error) :to-be-truthy)
      (expect (projection-state projection) :to-be 1)
      (expect (projection-checkpoint projection) :to-be 1))))
 (it
  "requires a store with global positions"
  (signals
   event-store-operation-not-supported
   (rebuild-projection
    (make-projection
     :initial-state
     nil
     :handler
     (lambda (state event)
       (declare (ignore event))
       state))
    (make-instance 'unsupported-store))))
  (signals
   event-store-operation-not-supported
   (advance-projection
    (make-projection
     :initial-state
     nil
     :handler
     (lambda (state event)
       (declare (ignore event))
       state))
    (make-instance 'unsupported-store))))
 (it
  "rejects malformed global feed entries without advancing"
  (let* ((projection
           (make-projection
            :initial-state
            :initial
            :handler
            (lambda (state event)
              (declare (ignore event))
              state)))
         (condition nil))
    (handler-case
        (rebuild-projection
         projection
         (make-instance 'malformed-projection-event-store))
      (projection-failure (caught) (setf condition caught)))
    (expect (typep condition 'projection-failure) :to-be-truthy)
    (expect (projection-failure-event condition) :to-be :not-an-event)
    (expect (projection-failure-global-position condition) :to-be nil)
    (expect (typep (projection-failure-cause condition) 'type-error)
            :to-be-truthy)
    (expect (projection-state projection) :to-be :initial)
    (expect (projection-checkpoint projection) :to-be 0)))
 (it
  "validates projection construction and checkpoint input"
  (signals type-error (make-projection :handler nil))
  (signals
   type-error
   (make-projection
    :initial-state-factory
    1
    :handler
    (lambda (state event)
      (declare (ignore event))
      state)))
  (signals
   type-error
   (make-projection
    :checkpoint
    -1
    :handler
    (lambda (state event)
      (declare (ignore event))
      state)))
  (signals
   type-error
   (rebuild-projection
    (make-projection
     :handler
     (lambda (state event)
       (declare (ignore event))
       state))
   (make-event-store)
    :from-global-position
    -1)))

(describe
 "restartable and defensive projection advancement"
 (it
  "rebuilds in bounded batches and resumes from the checkpoint"
  (let ((store (make-event-store))
        (projection
         (make-projection
          :initial-state
          0
          :handler
          (lambda (state event)
            (+ state (domain-event-payload event))))))
    (event-store-append
     store
     "projection-stream"
     (list
      (make-test-event "bounded-1" "projection-stream" 1)
      (make-test-event "bounded-2" "projection-stream" 2)
      (make-test-event "bounded-3" "projection-stream" 3))
     :expected-version
     :no-stream)
    (multiple-value-bind (state checkpoint)
        (rebuild-projection projection store :limit 2)
      (expect state :to-be 3)
      (expect checkpoint :to-be 2))
    (multiple-value-bind (state checkpoint)
        (advance-projection projection store :limit 1)
      (expect state :to-be 6)
      (expect checkpoint :to-be 3))
    (multiple-value-bind (state checkpoint)
        (advance-projection projection store)
      (expect state :to-be 6)
      (expect checkpoint :to-be 3))
    (multiple-value-bind (state checkpoint)
        (rebuild-projection projection store :limit 0)
      (expect state :to-be 0)
      (expect checkpoint :to-be 0))
    (signals type-error (advance-projection projection store :limit -1))
    (signals type-error (advance-projection projection store :limit :invalid))))
 (it
  "reports global-feed read failures without changing projection state"
  (let* ((projection
           (make-projection
            :initial-state
            :initial
            :handler
            (lambda (state event)
              (declare (ignore event))
              state)))
         (condition nil))
    (handler-case
        (advance-projection
         projection
         (make-instance 'projection-read-failure-store))
      (projection-read-failure (caught) (setf condition caught)))
    (expect condition :to-be-truthy)
    (expect (projection-read-failure-projection condition) :to-be projection)
    (expect (typep (projection-read-failure-cause condition) 'error)
            :to-be-truthy)
    (expect (projection-read-failure-after-global-position condition) :to-be 0)
    (expect (projection-read-failure-checkpoint condition) :to-be 0)
    (expect (projection-state projection) :to-be :initial)
    (expect (projection-checkpoint projection) :to-be 0)))
 (it
  "renders global-feed read failure details for operators"
  (let* ((cause (make-condition 'simple-error :format-control "read failed"))
         (condition
           (make-condition
            'projection-read-failure
            :projection :orders
            :cause cause
            :after-global-position 7
            :checkpoint 4))
         (rendered (format nil "~A" condition)))
    (expect (search "projection" rendered) :to-be-truthy)
    (expect (search "after-global-position" rendered) :to-be-truthy)
    (expect (search "checkpoint" rendered) :to-be-truthy)))
 (it
  "rejects a non-monotonic global feed after the last successful event"
  (let* ((projection
           (make-projection
            :initial-state
            0
            :handler
            (lambda (state event)
              (+ state (domain-event-payload event)))))
         (condition nil))
    (handler-case
        (rebuild-projection
         projection
         (make-instance 'nonmonotonic-global-position-store))
      (projection-failure (caught) (setf condition caught)))
    (expect condition :to-be-truthy)
    (expect
     (domain-event-id (projection-failure-event condition))
     :to-equal
     "nonmonotonic-2")
    (expect (projection-failure-global-position condition) :to-be 1)
    (expect (projection-failure-checkpoint condition) :to-be 1)
    (expect (projection-state projection) :to-be 1)
    (expect
     (invalid-domain-event-reason (projection-failure-cause condition))
     :to-be
     :global-position-order))))
