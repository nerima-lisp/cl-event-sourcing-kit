(defun in-memory-event-store-p (object)
  (typep object 'in-memory-event-store))

(defun make-in-memory-event-store (&key
                                   (lock *unspecified*)
                                   (global-position-start *unspecified*)
                                   (global-position-floor *unspecified*))
  "Create the thread-safe in-memory reference store.

The default lock is supplied by CL-CONCURRENT-KIT.  LOCK may be replaced with
any compatible lock object, which keeps deterministic tests and alternative
runtime synchronization injectable without changing store semantics."
  (let ((global-position-start
         (if (eq global-position-start *unspecified*) 0
           global-position-start))
        (global-position-floor
         (if (eq global-position-floor *unspecified*) 0
           global-position-floor)))
    (unless (and (integerp global-position-start) (<= 0 global-position-start))
      (error
       'type-error
       :datum
       global-position-start
       :expected-type
       '(integer 0 *)))
    (unless (and (integerp global-position-floor)
                 (<= 0 global-position-floor)
                 (<= global-position-floor global-position-start))
      (error
       'type-error
       :datum
       global-position-floor
       :expected-type
       '(integer 0 *)))
    (let ((resolved-lock
           (if (eq lock *unspecified*) (cl-concurrent-kit:make-lock
                                        :name
                                        "cl-event-sourcing-kit/in-memory")
             lock)))
      (unless (typep resolved-lock 'cl-concurrent-kit:lock)
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
       global-position-start
       :global-position-floor
       global-position-floor))))

(defun %in-memory-identity-key (value)
  "Return a stable EQUAL key for an in-memory stream or event identity.

Strings and cons trees are copied because callers can mutate those values
after an append.  Non-string arrays are rejected because the reference
adapter cannot make their contents a stable EQUAL hash key without choosing
a serialization format.  Other values retain their normal EQUAL identity
semantics; the adapter does not inspect or serialize opaque identity objects.
Circular cons trees are rejected rather than being used as hash keys."
  (cond
    ((stringp value)
     (copy-seq value))
    ((consp value)
     (labels ((copy-value (value active)
                (cond
                  ((consp value)
                   (when (gethash value active)
                     (%invalid-domain-event value :cyclic-identity))
                   (setf (gethash value active) t)
                   (unwind-protect
                        (cons (copy-value (car value) active)
                              (copy-value (cdr value) active))
                     (remhash value active)))
                  ((stringp value) (copy-seq value))
                  ((arrayp value)
                   (%invalid-domain-event value :mutable-identity))
                  (t value))))
       (copy-value value (make-hash-table :test #'eq))))
    ((arrayp value)
     (%invalid-domain-event value :mutable-identity))
    (t value)))

(defun make-event-store (&rest initargs)
  "Construct the reference in-memory store.

Persistent adapters should expose constructors in their own systems and
subclass EVENT-STORE directly; this convenience function never implies
durability."
  (apply #'make-in-memory-event-store initargs))

(defun %make-in-memory-event-order (events)
  (let ((order (make-array (max 16 (length events))
                           :adjustable t
                           :fill-pointer 0)))
    (dolist (event (reverse events) order)
      (vector-push-extend
       event
       order
       (max 64 (array-total-size order))))))

(defun %copy-in-memory-event-order (events)
  (let ((copy (make-array (max 16 (fill-pointer events))
                          :adjustable t
                          :fill-pointer 0)))
    (loop for event across events
          do (vector-push-extend
              event
              copy
              (max 64 (array-total-size copy))))
    copy))

(defun %in-memory-current-version-by-key-locked (store stream-key)
  (multiple-value-bind (version present-p)
      (gethash stream-key (%in-memory-stream-versions store))
    (if present-p
        (values version t)
        (multiple-value-bind (events stream-present-p)
            (gethash stream-key (%in-memory-streams store))
          (values
           (if stream-present-p (length events) 0)
           stream-present-p)))))

(defun %in-memory-current-version-locked (store stream-id)
  (%in-memory-current-version-by-key-locked
   store
   (%in-memory-identity-key stream-id)))

(defun %ensure-in-memory-stream-id (stream-id)
  (when (or (null stream-id) (eq stream-id *unspecified*))
    (%invalid-domain-event
     stream-id
     :stream-id
     "A stream id is required."))
  stream-id)

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
  (unless (%safe-equal-p (domain-event-stream-id event) stream-id)
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
   (%in-memory-identity-key (domain-event-id event))
   (domain-event-type event)
   (%in-memory-identity-key (domain-event-stream-id event))
   (%in-memory-identity-key (domain-event-aggregate-id event))
   (domain-event-payload event)
   (domain-event-metadata event)
   (domain-event-timestamp event)
   (domain-event-schema-version event)
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

(defun %classify-in-memory-append-events (store requested-events)
  (let ((seen (make-hash-table :test #'equal))
        (classifications nil)
        (new-events nil))
    ;; Classification happens while the store lock is held.  No store state is
    ;; changed until every request item has passed this phase.
    (dolist (requested requested-events)
      (let ((event-id (domain-event-id requested))
            (event-key (%in-memory-identity-key
                        (domain-event-id requested))))
        (multiple-value-bind (seen-event seen-p)
            (gethash event-key seen)
          (when seen-p
            (error 'duplicate-event-id
                   :event-id event-id
                   :existing-event seen-event
                   :requested-event requested))
          (setf (gethash event-key seen) requested)
          (multiple-value-bind (existing existing-p)
              (gethash event-key (%in-memory-event-index store))
            (if existing-p
                (if (event-store-event-equivalent-p store existing requested)
                    (push (list :duplicate existing) classifications)
                    (%signal-duplicate-conflict event-id existing requested))
                (progn
                  (%ensure-new-event-has-no-position requested)
                  (push (list :new requested) classifications)
                  (push requested new-events)))))))
    (values (nreverse classifications)
            (nreverse new-events))))

(defun %commit-in-memory-append-locked
    (store canonical-stream-id actual-version classifications)
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
    (let ((stream-events
            (gethash canonical-stream-id (%in-memory-streams store))))
      (dolist (committed committed-new-events)
        (push committed stream-events))
      (setf (gethash canonical-stream-id (%in-memory-streams store))
            stream-events
            (gethash canonical-stream-id (%in-memory-stream-versions store))
            (+ actual-version (length committed-new-events))))
    (dolist (committed committed-new-events)
      (setf (gethash (%in-memory-identity-key (domain-event-id committed))
                     (%in-memory-event-index store))
            committed))
    (let ((global-events (%in-memory-global-events store))
          (global-ordered-events (%in-memory-global-ordered-events store)))
      (dolist (committed committed-new-events)
        (push committed global-events))
      (dolist (committed committed-new-events)
        (vector-push-extend
         committed
         global-ordered-events
         (max 64 (array-total-size global-ordered-events))))
      (setf (%in-memory-global-events store) global-events
            (%in-memory-global-ordered-events store) global-ordered-events
            (%in-memory-global-position store) (1- next-global-position)))
    (values canonical-events
            (+ actual-version (length committed-new-events)))))

(defun %in-memory-append-locked (store stream-id requested-events expected-version)
  (let ((canonical-stream-id (%in-memory-identity-key stream-id)))
    (multiple-value-bind (actual-version exists-p)
        (%in-memory-current-version-by-key-locked store canonical-stream-id)
      (multiple-value-bind (classifications new-events)
          (%classify-in-memory-append-events store requested-events)
        (when (or (null requested-events) new-events)
          (unless (%expected-version-matches-p expected-version
                                                 exists-p
                                                 actual-version)
            (%signal-version-conflict stream-id
                                      expected-version
                                      actual-version)))
        (if (null new-events)
            (values (mapcar (lambda (classification)
                              (second classification))
                            classifications)
                    actual-version)
            (%commit-in-memory-append-locked
             store
             canonical-stream-id
             actual-version
             classifications))))))

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
    (%ensure-in-memory-stream-id stream-id)
    (let ((requested-events (%coerce-event-list events)))
      (dolist (event requested-events)
        (%validate-event-for-stream event stream-id))
      (%with-in-memory-lock (store)
        (%in-memory-append-locked
         store
         stream-id
         requested-events
         expected-version)))))
