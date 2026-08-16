(defun %file-store-stream (store)
  (or (%file-event-store-stream store)
      (setf (%file-event-store-stream store)
            (open (file-event-store-path store)
                  :direction :output
                  :if-exists :append
                  :if-does-not-exist :create))))

(defun %close-file-store-stream (store)
  (when (%file-event-store-stream store)
    (funcall (%file-event-store-sync store)
             (%file-event-store-stream store))
    (close (%file-event-store-stream store))
    (setf (%file-event-store-stream store) nil))
  t)

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
    (%close-file-store-stream store)))

(defun %serialized-file-log-record (store record)
  (let ((serialized
          (serialize-value record
                           :serializer (%file-event-store-serializer store))))
    (when (or (position #\Newline serialized)
              (position #\Return serialized))
      (error 'event-serialization-error
             :value record
             :direction :encode
             :cause "Event log records must be one line."))
    serialized))

(defun %file-log-record (store record)
  (write-line (%serialized-file-log-record store record)
              (%file-store-stream store))
  (funcall (%file-event-store-sync store) (%file-store-stream store))
  record)

(defun %replace-file-log-records (store records)
  (let* ((path (file-event-store-path store))
         (temporary (%durable-temporary-pathname path)))
    (unwind-protect
         (progn
           (with-open-file (stream temporary
                                   :direction :output
                                   :if-exists :supersede
                                   :if-does-not-exist :create)
             (dolist (record records)
               (write-line (%serialized-file-log-record store record)
                           stream))
             (funcall (%file-event-store-sync store) stream))
           (rename-file temporary path)
           t)
      (when (probe-file temporary)
        (delete-file temporary)))))

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
        (list nil nil)
        (with-open-file (stream path :direction :input)
          (let ((records nil)
                (incomplete-final-record-p nil))
            (loop
              (multiple-value-bind (line missing-newline-p)
                  (read-line stream nil nil)
                (when (null line)
                  (return))
                ;; A process may die in the middle of its final line.  The
                ;; prepare record is intentionally ignored until it is a
                ;; complete line; committed transactions are replayed from
                ;; complete prepare records.
                (if missing-newline-p
                    (setf incomplete-final-record-p t)
                    (handler-case
                        (push (deserialize-value
                               line
                               :serializer (%file-event-store-serializer store))
                              records)
                    (error (cause)
                      (%file-corruption store line cause))))))
            (list (reverse records) incomplete-final-record-p))))))

(defun %file-log-configuration-record (global-position-start)
  (list :configuration
        :global-position-start global-position-start))

(defun %file-log-configuration (store records)
  (let ((configuration-seen-p nil)
        (global-position-start nil))
    (dolist (record records)
      (when (and (%proper-list-p record)
                 (eq (first record) :configuration))
        (when configuration-seen-p
          (%file-corruption
           store
           record
           "A file log has duplicate configuration records."))
        (handler-case
            (progn
              (unless (and (oddp (length record))
                           (loop for tail on (rest record) by #'cddr
                                 always (keywordp (first tail))))
                (error "A configuration record is not a keyword plist."))
              (let ((candidate
                      (%required-wire-value record :global-position-start)))
                (unless (and (integerp candidate) (<= 0 candidate))
                  (error "A configured global position start is invalid."))
                (setf configuration-seen-p t
                      global-position-start candidate)))
          (error (cause)
            (%file-corruption store record cause)))))
    (values configuration-seen-p global-position-start)))

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
    (unless (and (integerp transaction-id) (plusp transaction-id))
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
          (max-transaction-id 0))
      (let* ((read-result (%read-file-log-records store))
             (records (first read-result))
             (incomplete-final-record-p (second read-result)))
        (multiple-value-bind (configuration-p configured-start)
          (%file-log-configuration store records)
          (let ((global-position-start
                  (%file-event-store-global-position-start store)))
            (when configuration-p
              (when (and (%file-event-store-global-position-start-supplied-p
                           store)
                         (/= configured-start global-position-start))
                (error 'event-sourcing-error
                       :message
                       (format nil
                               "File event store ~S has global position start ~D, not ~D."
                               (file-event-store-path store)
                               configured-start
                               global-position-start)))
              (setf global-position-start configured-start
                    (%file-event-store-global-position-start store)
                    global-position-start))
            (dolist (record records)
              (unless (and (%proper-list-p record)
                           (eq (first record) :configuration))
                (%validate-file-transaction-record
                 store record prepared committed aborted)
                (setf max-transaction-id
                      (max max-transaction-id
                           (%required-wire-value record :transaction-id)))))
            (setf (%file-event-store-delegate store)
                  (make-in-memory-event-store
                   :global-position-start global-position-start))
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
            (let ((configuration-record
                    (%file-log-configuration-record global-position-start)))
              (if incomplete-final-record-p
                  (progn
                    ;; The partial line is not a journal record.  Replace it
                    ;; only after validation and replay so a failed recovery
                    ;; leaves the original evidence intact.
                    (%close-file-store-stream store)
                    (%replace-file-log-records
                     store
                     (if configuration-p
                         records
                         (append records (list configuration-record)))))
                  (progn
                    (%file-store-stream store)
                    (unless configuration-p
                      (%file-log-record store configuration-record)))))
            store))))))
