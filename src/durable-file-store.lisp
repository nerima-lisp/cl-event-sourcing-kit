(defun %file-store-stream (store)
  (or (%file-event-store-stream store)
      (setf (%file-event-store-stream store)
            (open (file-event-store-path store)
                  :direction :output
                  :if-exists :append
                  :if-does-not-exist :create))))

(defun file-event-store-sync (store)
  "Flush the append log using the sync function supplied to the constructor."
  (unless (file-event-store-p store)
    (error 'type-error :datum store :expected-type 'file-event-store))
  (%with-durable-lock ((%file-event-store-lock store))
    (let ((stream (%file-store-stream store)))
      (funcall (%file-event-store-sync store) stream)
      t)))

(defun close-file-event-store (store)
  "Flush and close STORE.  A later operation reopens the append stream."
  (unless (file-event-store-p store)
    (error 'type-error :datum store :expected-type 'file-event-store))
  (%with-durable-lock ((%file-event-store-lock store))
    (when (%file-event-store-stream store)
      (funcall (%file-event-store-sync store)
               (%file-event-store-stream store))
      (close (%file-event-store-stream store))
      (setf (%file-event-store-stream store) nil))
    t))

(defun %file-log-record (store record)
  (let ((serialized
          (serialize-value record
                           :serializer (%file-event-store-serializer store))))
    (when (or (position #\Newline serialized)
              (position #\Return serialized))
      (error 'event-serialization-error
             :value record
             :direction :encode
             :cause "Event log records must be one line."))
    (write-line serialized (%file-store-stream store))
    (funcall (%file-event-store-sync store) (%file-store-stream store))
    record))

(defun %file-operation-events (wire-events)
  (unless (%proper-list-p wire-events)
    (error "A file append operation has an invalid event list."))
  (mapcar #'%domain-event-from-wire wire-events))

(defun %file-operation-requests (wire-requests)
  (unless (%proper-list-p wire-requests)
    (error "A file batch operation has an invalid request list."))
  (mapcar
   (lambda (request)
     (unless (and (%proper-list-p request) (eq (first request) :request))
       (error "A file batch operation has an invalid request."))
     (make-event-append-request
      :stream-id (%required-wire-value request :stream-id)
      :events (%file-operation-events (%required-wire-value request :events))
      :expected-version (%required-wire-value request :expected-version)))
   wire-requests))

(defun %apply-file-operation (delegate operation)
  (unless (and (%proper-list-p operation) (keywordp (first operation)))
    (error "A file transaction has an invalid operation."))
  (case (first operation)
    (:append
     (event-store-append
      delegate
      (%required-wire-value operation :stream-id)
      (%file-operation-events (%required-wire-value operation :events))
      :expected-version (%required-wire-value operation :expected-version)))
    (:batch
     (event-store-append-batch
      delegate
      (%file-operation-requests
       (%required-wire-value operation :requests))))
    (:snapshot
     (event-store-save-snapshot
      delegate
      (%snapshot-from-wire (%required-wire-value operation :snapshot))))
    (:delete-snapshot
     (event-store-delete-snapshot
      delegate
      (%required-wire-value operation :stream-id)))
    (:prune
     (event-store-prune
      delegate
      :before-global-position
      (%required-wire-value operation :before-global-position)))
    (otherwise (error "Unknown file transaction operation ~S." (first operation)))))

(defun %file-corruption (store record cause)
  (if (typep cause 'durable-store-corruption)
      (error cause)
      (error 'durable-store-corruption
             :path (file-event-store-path store)
             :record record
             :cause cause)))

(defun %read-file-log-records (store)
  (let ((path (file-event-store-path store)))
    (if (not (probe-file path))
        nil
        (with-open-file (stream path :direction :input)
          (let ((records nil))
            (loop
              (multiple-value-bind (line missing-newline-p)
                  (read-line stream nil nil)
                (when (null line)
                  (return))
                ;; A process may die in the middle of its final line.  The
                ;; prepare record is intentionally ignored until it is a
                ;; complete line; committed transactions are replayed from
                ;; complete prepare records.
                (unless missing-newline-p
                  (handler-case
                      (push (deserialize-value
                             line
                             :serializer (%file-event-store-serializer store))
                            records)
                    (error (cause)
                      (%file-corruption store line cause))))))
            (nreverse records))))))

(defun %validate-file-transaction-record
    (store record prepared committed aborted)
  (unless (and (%proper-list-p record)
               (keywordp (first record))
               (oddp (length record))
               (loop for tail on (rest record) by #'cddr
                     always (keywordp (first tail))))
    (%file-corruption store record "A log record is not a keyword plist."))
  (unless (member (first record) '(:prepare :commit :abort) :test #'eq)
    (%file-corruption store record "A log record has an unknown kind."))
  (let ((kind (first record))
        (transaction-id (%required-wire-value record :transaction-id)))
    (unless (and (integerp transaction-id) (> transaction-id 0))
      (%file-corruption store record "A transaction id is invalid."))
    (case kind
      (:prepare
       (when (gethash transaction-id prepared)
         (%file-corruption store record "A transaction has duplicate prepare records."))
       (unless (%required-wire-value record :operation)
         (%file-corruption store record "A prepare record has no operation."))
       (setf (gethash transaction-id prepared)
             (%required-wire-value record :operation)))
      (:commit
       (unless (gethash transaction-id prepared)
         (%file-corruption store record "A commit record has no prepare record."))
       (when (gethash transaction-id aborted)
         (%file-corruption store record "A transaction is both aborted and committed."))
       (when (gethash transaction-id committed)
         (%file-corruption store record "A transaction has duplicate commit records."))
       (setf (gethash transaction-id committed) t))
      (:abort
       (unless (gethash transaction-id prepared)
         (%file-corruption store record "An abort record has no prepare record."))
       (when (gethash transaction-id committed)
         (%file-corruption store record "A transaction is both committed and aborted."))
       (when (gethash transaction-id aborted)
         (%file-corruption store record "A transaction has duplicate abort records."))
       (setf (gethash transaction-id aborted) t)))))

(defun recover-file-event-store (store)
  "Rebuild STORE's delegate from complete prepare records in its log.

Committed and uncommitted prepares are replayed.  A prepare whose commit line
was lost is a transaction that may have reached the delegate before a crash;
replaying it is safe because the reference delegate has idempotent event IDs
and replacement snapshot versions."
  (unless (file-event-store-p store)
    (error 'type-error :datum store :expected-type 'file-event-store))
  (%with-durable-lock ((%file-event-store-lock store))
    (let ((prepared (make-hash-table :test #'eql))
          (committed (make-hash-table :test #'eql))
          (aborted (make-hash-table :test #'eql))
          (records (%read-file-log-records store))
          (max-transaction-id 0))
      (dolist (record records)
        (%validate-file-transaction-record
         store record prepared committed aborted)
        (setf max-transaction-id
              (max max-transaction-id
                   (%required-wire-value record :transaction-id))))
      (setf (%file-event-store-delegate store)
            (make-in-memory-event-store
             :global-position-start
             (%file-event-store-global-position-start store)))
      (let ((operations nil))
        (maphash
         (lambda (transaction-id operation)
           (unless (gethash transaction-id aborted)
             (push (cons transaction-id operation) operations)))
         prepared)
        (dolist (entry (sort operations #'< :key #'car))
          (handler-case
              (%apply-file-operation
               (%file-event-store-delegate store)
               (cdr entry))
            (error (cause)
              (%file-corruption store (cdr entry) cause)))))
      (setf (%file-event-store-next-transaction-id store)
            max-transaction-id)
      (%file-store-stream store)
      store)))

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
                   :next-transaction-id 0)))
      (recover-file-event-store store))))

(defun %file-log-abort (store transaction-id)
  (handler-bind
      ((error
         (lambda (cause)
           (warn "Failed to append abort record for durable transaction ~D: ~A"
                 transaction-id
                 cause)
           (return-from %file-log-abort nil))))
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
