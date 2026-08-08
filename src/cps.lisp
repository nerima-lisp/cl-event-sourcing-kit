(defun %call-continuation (thunk on-success on-error)
  (unless (functionp on-success)
    (error 'type-error :datum on-success :expected-type 'function))
  (unless (functionp on-error)
    (error 'type-error :datum on-error :expected-type 'function))
  (multiple-value-bind (completed-p result) (handler-case (values
                                                           t
                                                           (multiple-value-list
                                                            (funcall thunk)))
                                              (error (condition)
                                                (values nil condition)))
    (if completed-p (apply on-success result)
      (funcall on-error result))))

(defun event-store-append/cc (store
                              stream-id
                              events
                              on-success
                              &key
                              (on-error #'error)
                              (expected-version *unspecified*))
  "Synchronously append and invoke ON-SUCCESS or ON-ERROR.

This is a continuation-shaped API, not an asynchronous or scheduler-backed
guarantee.  Adapters can use the same shape to introduce asynchronous
transport at a later boundary without changing the core operation names."
  (let ((expected-version
         (if (eq expected-version *unspecified*) :any
           expected-version)))
    (%call-continuation
     (lambda ()
       (event-store-append
        store
        stream-id
        events
        :expected-version
        expected-version))
     on-success
     on-error)))

(defun event-store-read/cc (store
                            stream-id
                            on-success
                            &key
                            (on-error #'error)
                            from-version
                            to-version)
  "Synchronously read a stream through success/error continuations."
  (%call-continuation
   (lambda ()
     (event-store-read
      store
      stream-id
      :from-version
      from-version
      :to-version
      to-version))
   on-success
   on-error))

(defun replay-events/cc (initial-state
                         events
                         reducer
                         on-success
                         &key
                         (on-error #'error))
  "Synchronously replay through success/error continuations."
  (%call-continuation
   (lambda ()
     (replay-events initial-state events reducer))
   on-success
   on-error))

(defun commit-events/cc (staging
                         store
                         on-success
                         &key
                         (on-error #'error)
                         (expected-version *unspecified*))
  "Synchronously commit staging through success/error continuations."
  (%call-continuation
   (lambda ()
     (if (eq expected-version *unspecified*) (commit-events staging store)
       (commit-events staging store :expected-version expected-version)))
   on-success
   on-error))
