(defmethod event-store-current-version ((store in-memory-event-store) stream-id)
  (%ensure-in-memory-stream-id stream-id)
  (%with-in-memory-lock
   (store)
   (nth-value 0 (%in-memory-current-version-locked store stream-id))))

(defmethod event-store-current-global-position ((store in-memory-event-store))
  (%with-in-memory-lock (store) (%in-memory-global-position store)))

(defmethod event-store-stream-exists-p ((store in-memory-event-store) stream-id)
  (%ensure-in-memory-stream-id stream-id)
  (%with-in-memory-lock
   (store)
   (nth-value 1 (gethash (%in-memory-identity-key stream-id)
                         (%in-memory-streams store)))))

(defmethod event-store-global-position-supported-p ((store
                                                     in-memory-event-store))
  (declare (ignore store))
  t)

(defmethod event-store-prune ((store in-memory-event-store)
                              &key
                              (before-global-position *unspecified*))
  "Remove global-feed entries before BEFORE-GLOBAL-POSITION.

The stream history and snapshots remain available.  The floor is retained so
that a consumer which resumes from an obsolete cursor receives a typed
retention-gap condition instead of silently skipping events."
  (let ((boundary
          (if (eq before-global-position *unspecified*)
              0
            before-global-position)))
    (%validate-read-bound boundary)
    (%with-in-memory-lock
     (store)
     (let ((current-position (%in-memory-global-position store)))
       (when (> boundary (1+ current-position))
         (error
          'type-error
          :datum boundary
          :expected-type '(integer 0 *)))
       (setf (%in-memory-global-events store)
             (remove-if
              (lambda (event)
                (< (domain-event-global-position event) boundary))
              (%in-memory-global-events store)))
       (setf (%in-memory-global-ordered-events store)
             (%make-in-memory-event-order
              (%in-memory-global-events store))
             (%in-memory-global-position-floor store)
             (max boundary (%in-memory-global-position-floor store)))
       boundary))))

(defmethod event-store-retention-supported-p ((store in-memory-event-store))
  (declare (ignore store))
  t)

(defmethod event-store-retention-floor ((store in-memory-event-store))
  (%with-in-memory-lock (store)
    (%in-memory-global-position-floor store)))

(defmethod event-store-capabilities ((store in-memory-event-store))
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
     :process-local-lock)))
