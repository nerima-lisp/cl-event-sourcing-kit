(defun %call-continuation (thunk on-success on-error)
  (unless (functionp on-success)
    (error 'type-error :datum on-success :expected-type 'function))
  (unless (functionp on-error)
    (error 'type-error :datum on-error :expected-type 'function))
  (let ((outcome
          (cl-weave:with-continuation-values (values finish)
            (handler-case
                (multiple-value-call
                    (lambda (&rest next-values)
                      (apply #'finish :success next-values))
                  (funcall thunk))
              (error (condition)
                (funcall #'finish :error condition)))
            values)))
    (case (first outcome)
      (:success (apply on-success (rest outcome)))
      (:error (funcall on-error (second outcome))))))

(define-cps-operation event-store-append/cc
    (store
     stream-id
     events
     on-success
     &key
     (on-error #'error)
     (expected-version *unspecified*))
  "Synchronously append and invoke ON-SUCCESS or ON-ERROR.

This is a continuation-shaped API, not an asynchronous or scheduler-backed
guarantee.  A transport boundary can use the same shape to introduce
asynchronous execution without changing the core operation names."
  (let ((expected-version
         (if (eq expected-version *unspecified*) :any
           expected-version)))
    (event-store-append
     store
     stream-id
     events
     :expected-version
     expected-version)))

(define-cps-operation event-store-append-batch/cc
    (store
     requests
     on-success
     &key
     (on-error #'error))
  "Synchronously append a batch through success/error continuations."
  (event-store-append-batch store requests))

(define-cps-operation event-store-read/cc
    (store
     stream-id
     on-success
     &key
     (on-error #'error)
     from-version
     to-version)
  "Synchronously read a stream through success/error continuations."
  (event-store-read
   store
   stream-id
   :from-version
   from-version
   :to-version
   to-version))

(define-cps-operation replay-events/cc
    (initial-state
     events
     reducer
     on-success
     &key
     upcaster
     (on-error #'error))
  "Synchronously replay through success/error continuations."
  (replay-events initial-state events reducer :upcaster upcaster))

(define-cps-operation commit-events/cc
    (staging
     store
     on-success
     &key
     (on-error #'error)
     (expected-version *unspecified*))
  "Synchronously commit staging through success/error continuations."
  (if (eq expected-version *unspecified*) (commit-events staging store)
    (commit-events staging store :expected-version expected-version)))
