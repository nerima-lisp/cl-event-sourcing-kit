(defun %projection-failure (projection event checkpoint cause)
  (error
   'projection-failure
   :projection
   projection
   :event
   event
   :global-position
   (and event (domain-event-global-position event))
   :cause
   cause
   :checkpoint
   checkpoint))

(defun rebuild-projection (projection
                           store
                           &key
                           (from-global-position *unspecified*))
  "Reset PROJECTION and fold the global feed after a cursor.

The checkpoint advances only after the handler returns successfully.  A
handler error is wrapped in PROJECTION-FAILURE and leaves the last successful
checkpoint visible.  Core cannot roll back side effects performed by a
handler on externally mutable objects."
  (let ((from-global-position
         (if (eq from-global-position *unspecified*) 0
           from-global-position)))
    (check-type projection projection)
    (%validate-projection-checkpoint from-global-position)
    (unless (event-store-global-position-supported-p store)
      (error
       'event-store-operation-not-supported
       :operation
       :rebuild-projection
       :store
       store))
    (setf (slot-value projection 'state) (%fresh-projection-state projection)
          (slot-value projection 'checkpoint) from-global-position)
    (dolist (event
             (event-store-read-all
              store
              :after-global-position
              from-global-position))
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
        (handler-case (let ((next-state
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
             cause)))))
    (values (projection-state projection) (projection-checkpoint projection))))

(defun rebuild-projection/cc (projection
                              store
                              on-success
                              &key
                              (on-error #'error)
                              (from-global-position *unspecified*))
  "Synchronously rebuild a projection through success/error continuations."
  (%call-continuation
   (lambda ()
     (rebuild-projection
      projection
      store
      :from-global-position
      from-global-position))
   on-success
   on-error))
