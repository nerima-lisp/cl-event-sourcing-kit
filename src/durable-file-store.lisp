(defun make-file-event-store (&key
                              (path *unspecified*)
                              serializer
                              (sync #'finish-output)
                              (lock *unspecified*)
                              (global-position-start *unspecified*
                                                     global-position-start-supplied-p))
  "Create a restartable append-log reference event store.

The log is a portable line-oriented journal.  The in-memory delegate supplies
the protocol's validation and idempotency semantics; database adapters can
implement the same store generics and use the same serialized event envelope
without using this file implementation."
  (unless global-position-start-supplied-p
    (setf global-position-start 0))
  (when (eq path *unspecified*)
    (error 'event-sourcing-error :message "A file event store requires PATH."))
  (unless (functionp sync)
    (error 'type-error :datum sync :expected-type 'function))
  (unless (and (integerp global-position-start) (<= 0 global-position-start))
    (error 'type-error
           :datum global-position-start
           :expected-type '(integer 0 *)))
  (let ((resolved-lock
          (if (eq lock *unspecified*) (%durable-lock "cl-event-sourcing-kit/file")
            lock)))
    (unless (typep resolved-lock 'cl-concurrent-kit:lock)
      (error 'type-error
             :datum resolved-lock
             :expected-type 'cl-concurrent-kit:lock))
    (let* ((path (%durable-pathname path))
           (store (make-instance
                   'file-event-store
                   :path path
                   :serializer (%resolve-event-serializer serializer)
                   :delegate (make-in-memory-event-store
                              :global-position-start global-position-start)
                   :stream nil
                   :sync sync
                   :lock resolved-lock
                   :global-position-start global-position-start
                   :global-position-start-supplied-p
                   global-position-start-supplied-p
                   :next-transaction-id 0)))
      (recover-file-event-store store))))

(defun %file-log-abort (store transaction-id)
  (handler-bind
      ((error
         (lambda (cause)
           (warn "Failed to append abort record for durable transaction ~D: ~A"
                 transaction-id
                 cause)
           (return-from %file-log-abort))))
    (%file-log-record
     store
     (list :abort
           :kind :abort
           :transaction-id transaction-id)))
  nil)

(defun %file-transaction (store operation thunk)
  (%with-durable-lock ((%file-event-store-lock store))
    (let ((transaction-id
            (incf (%file-event-store-next-transaction-id store))))
      (%file-log-record
       store
       (list :prepare
             :kind :prepare
             :transaction-id transaction-id
             :operation operation))
      ;; Keep an application failure distinct from a commit-record failure.
      ;; A missing commit is replayable during recovery, while an explicit
      ;; abort must suppress the prepared operation on the next restart.
      (let ((values nil)
            (thunk-error nil))
        (handler-case
          (setf values (multiple-value-list (funcall thunk)))
          (error (cause)
            (setf thunk-error cause)))
        (if thunk-error
            (progn
              (%file-log-abort store transaction-id)
              (error thunk-error))
            (progn
              (%file-log-record
               store
               (list :commit
                     :kind :commit
                     :transaction-id transaction-id))
              (values-list values)))))))
