(in-package #:cl-event-sourcing-kit/test)

(define-event-reducer
 reduce-test-event
 (state event)
 (:increment (+ state (domain-event-payload event)))
 (:decrement (- state (domain-event-payload event)))
 (:otherwise state))

(define-event-reducer
 reduce-multi-key-event
 (state event)
 ((:alpha :beta) (+ state (domain-event-payload event)))
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
  "rejects CPS definitions without both continuation bindings"
  (signals error
    (macroexpand-1
     '(define-cps-operation missing-success (value on-error)
        (declare (ignore value on-error)))))
  (signals error
    (macroexpand-1
     '(define-cps-operation missing-error (value on-success)
        (declare (ignore value on-success))))))
 (it
  "validates both direct continuation functions and routes thunk failures"
  (signals type-error
    (cl-event-sourcing-kit::%call-continuation (lambda () :ok) nil #'identity))
  (signals type-error
    (cl-event-sourcing-kit::%call-continuation (lambda () :ok) #'identity nil))
  (let ((caught nil))
    (cl-event-sourcing-kit::%call-continuation
     (lambda () (error "continuation failure"))
     #'identity
     (lambda (condition) (setf caught condition)))
    (expect (typep caught 'error) :to-be-truthy)))
(it-each
 ((:increment 3 3)
  (:decrement 3 -3)
  (:unknown 3 0))
 "applies the ~A event case through the reducer"
 (event-type payload expected)
 (expect
  (reduce-test-event
   0
   (make-test-event
    (format nil "table-~A" event-type)
    "stream-1"
    payload
    :type
    event-type))
  :to-be
  expected))
 (it
  "preserves multiple event keys in reducer clauses"
  (let ((alpha (make-test-event "event-alpha" "stream-1" 3 :type :alpha))
        (beta (make-test-event "event-beta" "stream-1" 4 :type :beta))
        (unknown (make-test-event "event-unknown" "stream-1" 5 :type :unknown)))
    (expect (reduce-multi-key-event 0 alpha) :to-be 3)
    (expect (reduce-multi-key-event 0 beta) :to-be 4)
    (expect (reduce-multi-key-event 0 unknown) :to-be 0)))
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
  "routes replay upcasters through synchronous CPS"
  (let* ((event (make-test-event "upcast-1" "stream-1" 3))
         (upcasted (make-test-event "upcast-1" "stream-1" 7 :schema-version 2))
         (result nil)
         (failure nil))
    (replay-events/cc
     0
     (list event)
     (lambda (state current)
       (+ state (domain-event-payload current)))
     (lambda (state)
       (setf result state))
     :upcaster
     (lambda (current)
       (declare (ignore current))
       upcasted))
    (expect result :to-be 7)
    (replay-events/cc
     0
     (list event)
     #'identity
     (lambda (state)
       (declare (ignore state)))
     :upcaster
     (lambda (current)
       (declare (ignore current))
       :not-an-event)
     :on-error
     (lambda (condition)
       (setf failure condition)))
    (expect (typep failure 'invalid-domain-event) :to-be-truthy)))
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
  "runs batch append and incremental projection through synchronous CPS"
  (let* ((store (make-event-store))
         (batch-results nil)
         (batch-failure nil)
         (projection (make-test-projection))
         (advance-result nil)
         (advance-failure nil))
    (event-store-append-batch/cc
     store
     (list
      (make-event-append-request
       :stream-id
       "batch-cc"
       :events
       (list (make-test-event "batch-cc-1" "batch-cc" 7))
       :expected-version
       :no-stream))
     (lambda (results)
       (setf batch-results results)))
    (expect (length batch-results) :to-be 1)
    (expect (event-append-result-version (first batch-results)) :to-be 1)
    (advance-projection/cc
     projection
     store
     (lambda (state checkpoint)
       (setf advance-result (list state checkpoint))))
    (expect advance-result :to-equal '(7 1))
    (event-store-append-batch/cc
     store
     (list :not-a-request)
     (lambda (results)
       (declare (ignore results)))
     :on-error
     (lambda (condition)
       (setf batch-failure condition)))
    (expect (typep batch-failure 'type-error) :to-be-truthy)
    (advance-projection/cc
     projection
     (make-instance 'unsupported-store)
     (lambda (state checkpoint)
       (declare (ignore state checkpoint)))
     :on-error
     (lambda (condition)
       (setf advance-failure condition)))
    (expect
     (typep advance-failure 'event-store-operation-not-supported)
     :to-be-truthy)))
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
    (expect (typep failure 'projection-failure) :to-be-truthy)))
 (it
  "bridges a CPS result through cl-weave continuation bindings"
  (let ((store (make-event-store)))
    (event-store-append
     store
     "cps-stream"
     (list (make-test-event "cps-event-1" "cps-stream" :payload))
     :expected-version
     :no-stream)
    (with-continuation-result (events next calledp)
        (event-store-read/cc store "cps-stream" #'next)
      (expect calledp :to-be-truthy)
      (expect (mapcar #'domain-event-id events)
              :to-equal
              '("cps-event-1"))))))
