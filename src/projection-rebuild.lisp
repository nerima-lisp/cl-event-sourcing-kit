(defun %projection-failure (projection event checkpoint cause)
  (error
   'projection-failure
   :projection
   projection
   :event
   event
   :global-position
   (and (domain-event-p event) (domain-event-global-position event))
   :cause
   cause
   :checkpoint
   checkpoint))

(defun %projection-read-failure
       (projection after-global-position checkpoint cause)
  (error
   'projection-read-failure
   :projection
   projection
   :after-global-position
   after-global-position
   :checkpoint
   checkpoint
   :cause
   cause))

(defun %validate-projection-limit (limit)
  (unless (or (null limit) (and (integerp limit) (<= 0 limit)))
    (error 'type-error :datum limit :expected-type '(or null (integer 0 *))))
  limit)

(defun %projection-events (projection store after-global-position limit)
  (handler-case
      (%coerce-event-list
       (event-store-read-all
        store
        :after-global-position
        after-global-position
        :limit
        limit))
    (error (cause)
      (%projection-read-failure
       projection
       after-global-position
       (projection-checkpoint projection)
       cause))))

(defun %projection-event-position (projection event)
  (unless (domain-event-p event)
    (%projection-failure
     projection
     event
     (projection-checkpoint projection)
     (make-condition
      'type-error
      :datum
      event
      :expected-type
      'domain-event)))
  (let ((event-position (domain-event-global-position event)))
    (unless (and (integerp event-position) (<= 0 event-position))
      (%projection-failure
       projection
       event
       (projection-checkpoint projection)
       (make-condition
        'type-error
        :datum
        event-position
        :expected-type
        '(integer 0 *))))
    (when (<= event-position (projection-checkpoint projection))
      (%projection-failure
       projection
       event
       (projection-checkpoint projection)
       (make-condition
        'invalid-domain-event
        :event
        event
        :reason
        :global-position-order
        :message
        "The global position is not after the projection checkpoint.")))
    event-position))

(defun %advance-projection-events (projection store after-global-position limit)
  (dolist (event (%projection-events
                  projection
                  store
                  after-global-position
                  limit))
    (let ((event-position (%projection-event-position projection event)))
      (handler-case
          (let ((next-state
                  (funcall
                   (projection-handler projection)
                   (projection-state projection)
                   event)))
            (setf (slot-value projection 'state) next-state
                  (slot-value projection 'checkpoint) event-position))
        (error (cause)
          (%projection-failure
           projection
           event
           (projection-checkpoint projection)
           cause))))))

(defun rebuild-projection (projection
                           store
                           &key
                           (from-global-position *unspecified*)
                           limit)
  "Reset PROJECTION and fold the global feed after a cursor.

The checkpoint advances only after the handler returns successfully.  LIMIT
provides a bounded rebuild batch; call ADVANCE-PROJECTION to continue from the
resulting checkpoint.  Read failures and handler failures are reported as
structured conditions, and a handler error leaves the last successful
checkpoint visible.  Core cannot roll back side effects performed by a handler
on externally mutable objects."
  (let ((from-global-position
         (if (eq from-global-position *unspecified*) 0
           from-global-position)))
    (check-type projection projection)
    (%validate-projection-checkpoint from-global-position)
    (%validate-projection-limit limit)
    (unless (event-store-global-position-supported-p store)
      (error
       'event-store-operation-not-supported
       :operation
       :rebuild-projection
       :store
       store))
    (setf (slot-value projection 'state) (%fresh-projection-state projection)
          (slot-value projection 'checkpoint) from-global-position)
    (%advance-projection-events
     projection
     store
     from-global-position
     limit)
    (values (projection-state projection) (projection-checkpoint projection))))

(defun advance-projection (projection store &key limit)
  "Process at most LIMIT events after the projection's current checkpoint.

The state and checkpoint are preserved across calls, making this operation a
restartable synchronous catch-up boundary.  A live subscription, scheduler,
or durable checkpoint repository belongs to the adapter or application layer."
  (check-type projection projection)
  (%validate-projection-limit limit)
  (unless (event-store-global-position-supported-p store)
    (error
     'event-store-operation-not-supported
     :operation
     :advance-projection
     :store
     store))
  (%advance-projection-events
   projection
   store
   (projection-checkpoint projection)
   limit)
  (values (projection-state projection) (projection-checkpoint projection)))

(define-cps-operation rebuild-projection/cc
    (projection
     store
     on-success
     &key
     (on-error #'error)
     (from-global-position *unspecified*)
     limit)
  "Synchronously rebuild a projection through success/error continuations."
  (rebuild-projection
   projection
   store
   :from-global-position
   from-global-position
   :limit
   limit))

(define-cps-operation advance-projection/cc
    (projection
     store
     on-success
     &key
     (on-error #'error)
     limit)
  "Synchronously advance a projection through success/error continuations."
  (advance-projection projection store :limit limit))
