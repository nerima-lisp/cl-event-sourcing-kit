(in-package #:cl-event-sourcing-kit/test)

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
       :to-be
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
    -1))))
