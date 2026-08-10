(defun make-in-memory-event-outbox-store (&key
                                           (event-store *unspecified*)
                                           (outbox-store *unspecified*)
                                           lock)
  (let ((event-store
          (if (eq event-store *unspecified*)
              (make-in-memory-event-store)
            event-store))
        (outbox-store
          (if (eq outbox-store *unspecified*)
              (make-in-memory-outbox-store)
            outbox-store))
        (resolved-lock (or lock (%durable-lock "cl-event-sourcing-kit/event-outbox"))))
    (unless (in-memory-event-store-p event-store)
      (error 'type-error :datum event-store :expected-type 'in-memory-event-store))
    (unless (in-memory-outbox-store-p outbox-store)
      (error 'type-error :datum outbox-store :expected-type 'in-memory-outbox-store))
    (unless (typep resolved-lock 'cl-concurrent-kit:lock)
      (error 'type-error
             :datum resolved-lock
             :expected-type 'cl-concurrent-kit:lock))
    (make-instance 'event-outbox-store
                   :event-store event-store
                   :outbox-store outbox-store
                   :lock resolved-lock)))

(defun %capture-in-memory-event-state (store)
  (list (%copy-hash-table (%in-memory-streams store))
        (%copy-hash-table (%in-memory-event-index store))
        (%copy-hash-table (%in-memory-stream-versions store))
        (%copy-hash-table (%in-memory-snapshots store))
        (%in-memory-global-events store)
        (%copy-in-memory-event-order
         (%in-memory-global-ordered-events store))
        (%in-memory-global-position store)
        (%in-memory-global-position-floor store)))

(defun %restore-in-memory-event-state (store state)
  (setf (%in-memory-streams store) (first state)
        (%in-memory-event-index store) (second state)
        (%in-memory-stream-versions store) (third state)
        (%in-memory-snapshots store) (fourth state)
        (%in-memory-global-events store) (fifth state)
        (%in-memory-global-ordered-events store) (sixth state)
        (%in-memory-global-position store) (seventh state)
        (%in-memory-global-position-floor store) (eighth state))
  store)

(defmethod event-store-append-with-outbox ((store event-outbox-store)
                                           stream-id
                                           events
                                           messages
                                           &key
                                           (expected-version *unspecified*))
  (let ((event-store (%event-outbox-event-store store))
        (outbox-store (%event-outbox-outbox-store store))
        (messages (%coerce-event-list messages)))
    (unless (and (in-memory-event-store-p event-store)
                 (in-memory-outbox-store-p outbox-store))
      (error 'event-store-operation-not-supported
             :operation 'event-store-append-with-outbox
             :store store))
    (dolist (message messages) (%validate-outbox-message message))
    (%with-durable-lock ((%event-outbox-lock store))
      (let ((event-state (%capture-in-memory-event-state event-store))
            (outbox-state
              (list (copy-list (%in-memory-outbox-messages outbox-store))
                    (%copy-outbox-message-order
                     (%in-memory-outbox-ordered-messages outbox-store))
                    (%copy-hash-table (%in-memory-outbox-index outbox-store)))))
        (handler-case
            (multiple-value-bind (committed version)
                (event-store-append event-store stream-id events
                                    :expected-version expected-version)
              (dolist (message messages)
                (outbox-append outbox-store message))
              (values committed version))
          (error (cause)
            (%with-in-memory-lock (event-store)
              (%restore-in-memory-event-state event-store event-state))
            (%with-durable-lock ((%in-memory-outbox-lock outbox-store))
              (setf (slot-value outbox-store 'messages) (first outbox-state)
                    (slot-value outbox-store 'ordered-messages) (second outbox-state)
                    (slot-value outbox-store 'message-index) (third outbox-state)))
            (error cause)))))))

(defmethod event-store-append-with-outbox
    ((store event-store) stream-id events messages &key expected-version)
  (declare (ignore stream-id events messages expected-version))
  (error 'event-store-operation-not-supported
         :operation 'event-store-append-with-outbox
         :store store))

(defmethod event-store-capabilities ((store event-outbox-store))
  (remove-duplicates
   (cons :atomic-outbox
         (event-store-capabilities (%event-outbox-event-store store)))
   :test #'eq))
