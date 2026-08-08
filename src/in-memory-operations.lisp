(defun in-memory-event-store-p (object)
  (typep object 'in-memory-event-store))

(defun make-in-memory-event-store (&key
                                   (lock *unspecified*)
                                   (global-position-start *unspecified*))
  "Create the thread-safe in-memory reference store.

The default lock is supplied by CL-CONCURRENT-KIT.  LOCK may be replaced with
any compatible lock object, which keeps deterministic tests and alternative
runtime synchronization injectable without changing store semantics."
  (let ((global-position-start
         (if (eq global-position-start *unspecified*) 0
           global-position-start)))
    (unless (and (integerp global-position-start) (<= 0 global-position-start))
      (error
       'type-error
       :datum
       global-position-start
       :expected-type
       '(integer 0 *)))
    (let ((resolved-lock
           (if (eq lock *unspecified*) (cl-concurrent-kit:make-lock
                                        :name
                                        "cl-event-sourcing-kit/in-memory")
             lock)))
      (unless resolved-lock
        (error
         'type-error
         :datum
         resolved-lock
         :expected-type
         'cl-concurrent-kit:lock))
      (make-instance
       'in-memory-event-store
       :lock
       resolved-lock
       :global-position-start
       global-position-start))))

(defun make-event-store (&rest initargs)
  "Construct the reference in-memory store.

Persistent adapters should expose constructors in their own systems and
subclass EVENT-STORE directly; this convenience function never implies
durability."
  (apply #'make-in-memory-event-store initargs))

(defun %in-memory-current-version-locked (store stream-id)
  (multiple-value-bind (events present-p) (gethash
                                           stream-id
                                           (%in-memory-streams store))
    (values
     (if present-p (length events)
       0)
     present-p)))

(defun %validate-read-bound (value)
  (unless (or (null value) (and (integerp value) (<= 0 value)))
    (error 'type-error :datum value :expected-type '(or null (integer 0 *))))
  value)

(defun %validate-limit (limit)
  (unless (or (null limit) (and (integerp limit) (<= 0 limit)))
    (error 'type-error :datum limit :expected-type '(or null (integer 0 *))))
  limit)

(defun %validate-event-for-stream (event stream-id)
  (unless (domain-event-p event)
    (%invalid-domain-event event :event-type))
  (unless (equal (domain-event-stream-id event) stream-id)
    (%invalid-domain-event
     event
     :stream-id
     "The event belongs to a different stream."))
  event)

(defun %ensure-new-event-has-no-position (event)
  (when (or
         (not (null (domain-event-version event)))
         (not (null (domain-event-global-position event))))
    (%invalid-domain-event
     event
     :assigned-position
     "A new event must not already have a store position."))
  event)

(defun %committed-copy (event version global-position)
  (%make-domain-event
   (domain-event-id event)
   (domain-event-type event)
   (domain-event-stream-id event)
   (domain-event-aggregate-id event)
   (domain-event-payload event)
   (domain-event-metadata event)
   (domain-event-timestamp event)
   version
   (domain-event-correlation-id event)
   (domain-event-causation-id event)
   global-position))

(defun %signal-duplicate-conflict (event-id existing requested)
  (error
   'duplicate-event-id-conflict
   :event-id
   event-id
   :existing-event
   existing
   :requested-event
   requested))

(defmethod event-store-append ((store in-memory-event-store) stream-id events
                               &key (expected-version *unspecified*))
  "Atomically append EVENTS to STREAM-ID.

Exact existing event ids are idempotent.  An all-duplicate retry bypasses the
expected-version check so a client can safely retry after an unknown result;
an append containing any new event still requires its expected version to
match.  New versions and global positions are allocated only after every
validation succeeds."
  (let ((expected-version
          (if (eq expected-version *unspecified*) :any expected-version)))
    (%validate-expected-version expected-version)
    (unless stream-id
      (%invalid-domain-event nil :stream-id "An append stream id is required."))
    (let ((requested-events (%coerce-event-list events)))
      (dolist (event requested-events)
        (%validate-event-for-stream event stream-id))
      (%with-in-memory-lock (store)
        (multiple-value-bind (actual-version exists-p)
            (%in-memory-current-version-locked store stream-id)
          (let ((seen (make-hash-table :test #'equal))
                (classifications nil)
                (new-events nil))
          ;; Classify the entire request while the lock is held.  No state is
          ;; changed until this pass and the optimistic check complete.
          (dolist (requested requested-events)
            (let ((event-id (domain-event-id requested)))
              (multiple-value-bind (seen-event seen-p)
                  (gethash event-id seen)
                (when seen-p
                  (error 'duplicate-event-id
                         :event-id event-id
                         :existing-event seen-event
                         :requested-event requested))
                (setf (gethash event-id seen) requested)
                (multiple-value-bind (existing existing-p)
                    (gethash event-id (%in-memory-event-index store))
                  (if existing-p
                      (if (event-store-event-equivalent-p
                           store existing requested)
                          (push (list :duplicate existing) classifications)
                          (%signal-duplicate-conflict event-id existing requested))
                      (progn
                        (%ensure-new-event-has-no-position requested)
                        (push (list :new requested) classifications)
                        (push requested new-events)))))))
          (setf classifications (nreverse classifications)
                new-events (nreverse new-events))
          (when (or (null requested-events) new-events)
            (unless (%expected-version-matches-p expected-version
                                                   exists-p
                                                   actual-version)
              (%signal-version-conflict stream-id expected-version actual-version)))
          (if (null new-events)
              (values (mapcar (lambda (classification)
                                (second classification))
                              classifications)
                      actual-version)
              (let ((committed-new-events nil)
                    (canonical-events nil)
                    (next-version (1+ actual-version))
                    (next-global-position
                      (1+ (%in-memory-global-position store))))
                (dolist (classification classifications)
                  (if (eq (first classification) :duplicate)
                      (push (second classification) canonical-events)
                      (let ((committed
                              (%committed-copy (second classification)
                                               next-version
                                               next-global-position)))
                        (push committed canonical-events)
                        (push committed committed-new-events)
                        (incf next-version)
                        (incf next-global-position))))
                (setf canonical-events (nreverse canonical-events)
                      committed-new-events (nreverse committed-new-events))
                (setf (gethash stream-id (%in-memory-streams store))
                      (append (or (gethash stream-id (%in-memory-streams store))
                                  nil)
                              committed-new-events))
                (dolist (committed committed-new-events)
                  (setf (gethash (domain-event-id committed)
                                 (%in-memory-event-index store))
                        committed))
                (setf (%in-memory-global-events store)
                      (append (%in-memory-global-events store)
                              committed-new-events)
                      (%in-memory-global-position store)
                      (1- next-global-position))
                  (values canonical-events
                          (+ actual-version (length committed-new-events)))))))))))

(defmethod event-store-read ((store in-memory-event-store)
                             stream-id
                             &key
                             from-version
                             to-version)
  "Read STREAM-ID in ascending inclusive version order.

FROM-VERSION and TO-VERSION are inclusive when supplied.  A missing stream
returns NIL, and omitted FROM-VERSION reads from the first version."
  (%validate-read-bound from-version)
  (%validate-read-bound to-version)
  (%with-in-memory-lock
   (store)
   (let ((events (gethash stream-id (%in-memory-streams store))))
     (copy-list
      (remove-if-not
       (lambda (event)
         (and
          (or
           (null from-version)
           (<= from-version (domain-event-version event)))
          (or (null to-version) (<= (domain-event-version event) to-version))))
       events)))))

(defmethod event-store-read-all ((store in-memory-event-store)
                                 &key
                                 (after-global-position *unspecified*)
                                 limit)
  "Read the global feed strictly after AFTER-GLOBAL-POSITION."
  (let ((after-global-position
         (if (eq after-global-position *unspecified*) 0
           after-global-position)))
    (%validate-read-bound after-global-position)
    (%validate-limit limit)
    (%with-in-memory-lock
     (store)
     (let* ((cursor (or after-global-position 0))
            (events
             (remove-if-not
              (lambda (event)
                (> (domain-event-global-position event) cursor))
              (%in-memory-global-events store))))
       (copy-list
        (if limit (subseq events 0 (min limit (length events)))
          events))))))

(defmethod event-store-current-version ((store in-memory-event-store) stream-id)
  (%with-in-memory-lock
   (store)
   (nth-value 0 (%in-memory-current-version-locked store stream-id))))

(defmethod event-store-current-global-position ((store in-memory-event-store))
  (%with-in-memory-lock (store) (%in-memory-global-position store)))

(defmethod event-store-stream-exists-p ((store in-memory-event-store) stream-id)
  (%with-in-memory-lock
   (store)
   (nth-value 1 (gethash stream-id (%in-memory-streams store)))))

(defmethod event-store-global-position-supported-p ((store
                                                     in-memory-event-store))
  (declare (ignore store))
  t)
