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
the protocol's validation and idempotency semantics; database backend implementations can
implement the same store generics and use the same serialized event envelope without using
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
