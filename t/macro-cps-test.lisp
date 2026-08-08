(in-package #:cl-event-sourcing-kit/test)

(define-event-reducer
 reduce-test-event
 (state event)
 (:increment (+ state (domain-event-payload event)))
 (:decrement (- state (domain-event-payload event)))
 (:otherwise state))

(define-projection
 make-test-projection
 :initial-state
 0
 :handler
 (lambda (state event)
   (+ state (domain-event-payload event))))

(describe
 "macro and continuation interfaces"
 (it
  "defines a readable event-type reducer"
  (let ((increment (make-test-event "event-1" "stream-1" 3 :type :increment))
        (decrement (make-test-event "event-2" "stream-1" 1 :type :decrement))
        (unknown (make-test-event "event-3" "stream-1" 1 :type :unknown)))
    (expect (reduce-test-event 0 increment) :to-be 3)
    (expect (reduce-test-event 3 decrement) :to-be 2)
    (expect (reduce-test-event 2 unknown) :to-be 2)))
 (it
  "builds a projection with the projection macro"
  (let ((projection (make-test-projection)))
    (expect (projection-p projection) :to-be-truthy)
    (expect (projection-name projection) :to-be 'make-test-projection)))
 (it
  "runs append, read, replay, and commit through synchronous CPS"
  (let* ((store (make-event-store))
         (staging (make-event-staging "stream-1" :expected-version 1))
         (event (make-test-event "event-1" "stream-1" 5))
         (append-version nil)
         (read-events nil)
         (replayed nil)
         (committed-version nil))
    (event-store-append/cc
     store
     "stream-1"
     (list event)
     (lambda (events version)
       (declare (ignore events))
       (setf append-version version))
     :expected-version
     :no-stream)
    (event-store-read/cc
     store
     "stream-1"
     (lambda (events)
       (setf read-events events)))
    (replay-events/cc
     0
     read-events
     (lambda (state current)
       (+ state (domain-event-payload current)))
     (lambda (state)
       (setf replayed state)))
    (stage-event staging (make-test-event "event-2" "stream-1" 2))
    (commit-events/cc
     staging
     store
     (lambda (events version)
       (declare (ignore events))
       (setf committed-version version)))
    (expect append-version :to-be 1)
    (expect replayed :to-be 5)
    (expect committed-version :to-be 2)
    (expect (uncommitted-events staging) :to-be nil)))
 (it
  "routes synchronous CPS failures to the error continuation"
  (let* ((store (make-event-store))
         (condition nil))
    (event-store-append/cc
     store
     "stream-1"
     (list (make-test-event "event-1" "stream-1" 1))
     (lambda (events version)
       (declare (ignore events version)))
     :expected-version
     :no-stream)
    (event-store-append/cc
     store
     "stream-1"
     (list (make-test-event "event-2" "stream-1" 2))
     (lambda (events version)
       (declare (ignore events version)))
     :expected-version
     0
     :on-error
     (lambda (caught)
       (setf condition caught)))
    (expect (typep condition 'event-version-conflict) :to-be-truthy)
    (signals type-error (event-store-read/cc store "stream-1" nil))))
 (it
  "routes projection CPS success and failure"
  (let ((store (make-event-store))
        (state nil)
        (failure nil))
    (event-store-append
     store
     "stream-1"
     (list (make-test-event "event-1" "stream-1" 4))
     :expected-version
     :no-stream)
    (rebuild-projection/cc
     (make-test-projection)
     store
     (lambda (new-state checkpoint)
       (setf state (list new-state checkpoint))))
    (rebuild-projection/cc
     (make-projection
      :initial-state
      0
      :handler
      (lambda (current event)
        (declare (ignore current event))
        (error "projection failure")))
     store
     (lambda (new-state checkpoint)
       (declare (ignore new-state checkpoint)))
     :on-error
     (lambda (condition)
       (setf failure condition)))
    (expect state :to-equal '(4 1))
    (expect (typep failure 'projection-failure) :to-be-truthy))))
