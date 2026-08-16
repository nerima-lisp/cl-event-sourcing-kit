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
subclass EVENT-STORE and use the same serialized event envelope without using
this file implementation."
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

(defmethod event-store-append ((store file-event-store)
                               stream-id
                               events
                               &key
                               (expected-version *unspecified*))
  (let* ((expected-version
           (if (eq expected-version *unspecified*) :any expected-version))
         (events (%coerce-event-list events))
         (operation
           (list :append
                 :stream-id stream-id
                 :events (mapcar #'%domain-event-wire events)
                 :expected-version expected-version)))
    (%validate-expected-version expected-version)
    (%file-transaction
     store operation
     (lambda ()
       (event-store-append
        (%file-event-store-delegate store)
        stream-id
        events
        :expected-version expected-version)))))

(defmethod event-store-append-batch ((store file-event-store) requests)
  (let* ((requests (%coerce-event-list requests))
         (operation
           (list :batch
                 :requests
                 (mapcar
                  (lambda (request)
                    (unless (event-append-request-p request)
                      (error 'type-error
                             :datum request
                             :expected-type 'event-append-request))
                    (list :request
                          :stream-id (event-append-request-stream-id request)
                          :events
                          (mapcar #'%domain-event-wire
                                  (%coerce-event-list
                                   (event-append-request-events request)))
                          :expected-version
                          (event-append-request-expected-version request)))
                  requests))))
    (%file-transaction
     store operation
     (lambda ()
       (event-store-append-batch (%file-event-store-delegate store)
                                  requests)))))

(defmethod event-store-save-snapshot ((store file-event-store) snapshot)
  (let ((operation (list :snapshot :snapshot (%snapshot-wire snapshot))))
    (%file-transaction
     store operation
     (lambda ()
       (event-store-save-snapshot (%file-event-store-delegate store)
                                  snapshot)))))

(defmethod event-store-delete-snapshot ((store file-event-store) stream-id)
  (%file-transaction
   store
   (list :delete-snapshot :stream-id stream-id)
   (lambda ()
     (event-store-delete-snapshot (%file-event-store-delegate store)
                                  stream-id))))

(defmethod event-store-read ((store file-event-store)
                             stream-id
                             &key
                             from-version
                             to-version)
  (%with-durable-lock ((%file-event-store-lock store))
    (event-store-read (%file-event-store-delegate store)
                      stream-id
                      :from-version from-version
                      :to-version to-version)))

(defmethod event-store-read-all ((store file-event-store)
                                 &key
                                 (after-global-position *unspecified*)
                                 limit)
  (%with-durable-lock ((%file-event-store-lock store))
    (event-store-read-all
     (%file-event-store-delegate store)
     :after-global-position after-global-position
     :limit limit)))

(defmethod event-store-current-version ((store file-event-store) stream-id)
  (%with-durable-lock ((%file-event-store-lock store))
    (event-store-current-version (%file-event-store-delegate store) stream-id)))

(defmethod event-store-current-global-position ((store file-event-store))
  (%with-durable-lock ((%file-event-store-lock store))
    (event-store-current-global-position (%file-event-store-delegate store))))

(defmethod event-store-stream-exists-p ((store file-event-store) stream-id)
  (%with-durable-lock ((%file-event-store-lock store))
    (event-store-stream-exists-p (%file-event-store-delegate store) stream-id)))

(defmethod event-store-global-position-supported-p ((store file-event-store))
  (declare (ignore store))
  t)

(defmethod event-store-event-equivalent-p ((store file-event-store)
                                           existing-event
                                           requested-event)
  (event-store-event-equivalent-p (%file-event-store-delegate store)
                                  existing-event
                                  requested-event))

(defmethod event-store-read-snapshot ((store file-event-store)
                                      stream-id
                                      &key
                                      version)
  (%with-durable-lock ((%file-event-store-lock store))
    (event-store-read-snapshot (%file-event-store-delegate store)
                               stream-id
                               :version version)))

(defmethod event-store-snapshots-supported-p ((store file-event-store))
  (declare (ignore store))
  t)

(defmethod event-store-prune ((store file-event-store)
                              &key
                              (before-global-position *unspecified*))
  (let ((boundary
          (if (eq before-global-position *unspecified*)
              0
            before-global-position)))
    (%validate-read-bound boundary)
    (%file-transaction
     store
     (list :prune :before-global-position boundary)
     (lambda ()
       (event-store-prune (%file-event-store-delegate store)
                          :before-global-position boundary)))))

(defmethod event-store-retention-supported-p ((store file-event-store))
  (declare (ignore store))
  t)

(defmethod event-store-retention-floor ((store file-event-store))
  (%with-durable-lock ((%file-event-store-lock store))
    (event-store-retention-floor (%file-event-store-delegate store))))

(defmethod event-store-capabilities ((store file-event-store))
  (declare (ignore store))
  (copy-list
   '(:append
     :read
     :read-all
     :optimistic-concurrency
     :idempotent-event-ids
     :batch-append
     :global-position
     :snapshots
     :retention
     :durable
     :crash-recovery
     :process-local-lock)))
