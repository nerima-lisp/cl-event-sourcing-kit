(defun %validate-append-request (request)
  (unless (event-append-request-p request)
    (error
     'type-error
     :datum request
     :expected-type 'event-append-request))
  (let ((stream-id (event-append-request-stream-id request))
        (expected-version (event-append-request-expected-version request)))
    (%ensure-in-memory-stream-id stream-id)
    (%validate-expected-version expected-version)
    (let ((events (%coerce-event-list (event-append-request-events request))))
      (dolist (event events)
        (%validate-event-for-stream event stream-id))
      (values stream-id events expected-version))))

(defun %copy-hash-table (table)
  (let ((copy (make-hash-table :test (hash-table-test table))))
    (maphash (lambda (key value) (setf (gethash key copy) value)) table)
    copy))

(defmethod event-store-append-batch ((store in-memory-event-store) requests)
  "Atomically append a list or vector of per-stream requests.

Every request is validated before the first request is committed.  If a later
request fails, the reference store restores all stream, id, and global-feed
indexes to their state before the batch began."
  (let ((validated nil))
    (dolist (request (%coerce-event-list requests))
      (multiple-value-bind (stream-id events expected-version)
          (%validate-append-request request)
        (push (list stream-id events expected-version) validated)))
    (setf validated (nreverse validated))
    (%with-in-memory-lock
     (store)
     (let ((streams (%copy-hash-table (%in-memory-streams store)))
           (event-index (%copy-hash-table (%in-memory-event-index store)))
           (stream-versions
             (%copy-hash-table (%in-memory-stream-versions store)))
           (global-events (%in-memory-global-events store))
           (global-ordered-events
             (%copy-in-memory-event-order
              (%in-memory-global-ordered-events store)))
           (global-position (%in-memory-global-position store))
           (global-position-floor
             (%in-memory-global-position-floor store)))
       (handler-case
           (mapcar
            (lambda (request)
              (destructuring-bind (stream-id events expected-version) request
                (multiple-value-bind (committed version)
                    (%in-memory-append-locked
                     store
                     stream-id
                     events
                     expected-version)
                  (%make-event-append-result committed version))))
            validated)
         (error (condition)
           (setf (%in-memory-streams store) streams
                 (%in-memory-event-index store) event-index
                 (%in-memory-stream-versions store) stream-versions
                 (%in-memory-global-events store) global-events
                 (%in-memory-global-ordered-events store)
                 global-ordered-events
                 (%in-memory-global-position store) global-position
                 (%in-memory-global-position-floor store) global-position-floor)
           (error condition)))))))
