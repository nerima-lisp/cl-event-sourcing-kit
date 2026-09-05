;;;; Versioned upcasters and operational integration

(defun make-upcaster-registry (&key table)
  (let ((table (or table (make-hash-table :test #'equal))))
    (unless (hash-table-p table)
      (error 'type-error :datum table :expected-type 'hash-table))
    (unless (eq (hash-table-test table) 'equal)
      (error 'event-sourcing-error
             :message "An upcaster registry requires an EQUAL hash table."))
    (make-instance 'upcaster-registry :table table)))

(defun %validate-upcaster-version (version description)
  (unless (and (integerp version) (<= 0 version))
    (error 'event-sourcing-error :message description))
  version)

(defmethod register-upcaster ((registry upcaster-registry)
                              event-type
                              from-version
                              to-version
                              function)
  (%validate-durable-key
   event-type
   "An upcaster requires a non-NIL event type.")
  (%validate-upcaster-version
   from-version
   "An upcaster source version must be a non-negative integer.")
  (%validate-upcaster-version
   to-version
   "An upcaster target version must be a non-negative integer.")
  (unless (< from-version to-version)
    (error 'event-sourcing-error
           :message "An upcaster target version must be greater than its source version."))
  (unless (functionp function)
    (error 'type-error :datum function :expected-type 'function))
  (let ((key (list event-type from-version))
        (table (%upcaster-table registry)))
    (when (nth-value 1 (gethash key table))
      (error 'event-sourcing-error
             :message "An upcaster for this event type and source version is already registered."))
    (setf (gethash key table) (cons to-version function)))
  registry)

(defmethod upcaster-registry-function ((registry upcaster-registry)
                                       &key event-type)
  (when (and event-type (eq event-type *unspecified*))
    (error 'event-sourcing-error
           :message "An upcaster event type cannot be unspecified."))
  (lambda (event)
    (unless (domain-event-p event)
      (%invalid-domain-event event :upcaster-input))
    (if (and event-type
             (not (%safe-equal-p event-type (domain-event-type event))))
        event
        (let ((current event)
              (seen (make-hash-table :test #'equal)))
          (loop
            (let* ((key (list (domain-event-type current)
                              (domain-event-schema-version current)))
                   (entry (gethash key (%upcaster-table registry))))
              (unless entry
                (return current))
              (when (nth-value 1 (gethash key seen))
                (error 'event-sourcing-error
                       :message "The upcaster registry contains a cycle."))
              (setf (gethash key seen) t)
              (let* ((target-version (car entry))
                     (next (funcall (cdr entry) current)))
                (unless (domain-event-p next)
                  (%invalid-domain-event next :upcaster-result))
                (unless (and (%safe-equal-p (domain-event-type current)
                                            (domain-event-type next))
                             (= target-version
                                (domain-event-schema-version next))
                             (%upcaster-envelope-preserved-p current next))
                  (error 'event-sourcing-error
                         :message "An upcaster must preserve the event envelope and return its registered target schema version."))
                (setf current next))))))))

(defun %observed-notify (callback &rest arguments)
  (when callback
    (handler-bind
        ((error
           (lambda (cause)
             (warn "Event-store observation callback failed: ~A" cause)
             (return-from %observed-notify nil))))
      (apply callback arguments)))
  nil)

(defun %observed-call (store operation function)
  (%observed-notify (%observed-event-store-before store) operation)
  (handler-case
      (multiple-value-prog1
          (funcall function)
        (%observed-notify (%observed-event-store-after store) operation))
    (error (cause)
      (%observed-notify (%observed-event-store-on-error store)
                        operation
                        cause)
      (error cause))))

(defmethod event-store-append ((store observed-event-store)
                               stream-id
                               events
                               &key
                               (expected-version *unspecified*))
  (%observed-call
   store
   :append
   (lambda ()
     (event-store-append (%observed-event-store-delegate store)
                          stream-id
                          events
                          :expected-version expected-version))))

(defmethod event-store-read ((store observed-event-store)
                             stream-id
                             &key
                             from-version
                             to-version)
  (%observed-call
   store
   :read
   (lambda ()
     (event-store-read (%observed-event-store-delegate store)
                       stream-id
                       :from-version from-version
                       :to-version to-version))))

(defmethod event-store-read-all ((store observed-event-store)
                                 &key
                                 (after-global-position *unspecified*)
                                 limit)
  (%observed-call
   store
   :read-all
   (lambda ()
     (event-store-read-all (%observed-event-store-delegate store)
                           :after-global-position after-global-position
                           :limit limit))))

(defmethod event-store-current-version ((store observed-event-store) stream-id)
  (%observed-call
   store
   :current-version
   (lambda ()
     (event-store-current-version (%observed-event-store-delegate store)
                                  stream-id))))

(defmethod event-store-current-global-position ((store observed-event-store))
  (%observed-call
   store
   :current-global-position
   (lambda ()
     (event-store-current-global-position
      (%observed-event-store-delegate store)))))

(defmethod event-store-stream-exists-p ((store observed-event-store) stream-id)
  (%observed-call
   store
   :stream-exists-p
   (lambda ()
     (event-store-stream-exists-p (%observed-event-store-delegate store)
                                  stream-id))))

(defmethod event-store-global-position-supported-p
    ((store observed-event-store))
  (%observed-call
   store
   :global-position-supported-p
   (lambda ()
     (event-store-global-position-supported-p
      (%observed-event-store-delegate store)))))

(defmethod event-store-event-equivalent-p ((store observed-event-store)
                                           existing-event
                                           requested-event)
  (%observed-call
   store
   :event-equivalent-p
   (lambda ()
     (event-store-event-equivalent-p
      (%observed-event-store-delegate store)
      existing-event
      requested-event))))

(defmethod event-store-append-batch ((store observed-event-store) requests)
  (%observed-call
   store
   :append-batch
   (lambda ()
     (event-store-append-batch
      (%observed-event-store-delegate store)
      requests))))

(defmethod event-store-save-snapshot ((store observed-event-store) snapshot)
  (%observed-call
   store
   :save-snapshot
   (lambda ()
     (event-store-save-snapshot
      (%observed-event-store-delegate store)
      snapshot))))

(defmethod event-store-read-snapshot ((store observed-event-store)
                                      stream-id
                                      &key
                                      version)
  (%observed-call
   store
   :read-snapshot
   (lambda ()
     (event-store-read-snapshot
      (%observed-event-store-delegate store)
      stream-id
      :version version))))

(defmethod event-store-delete-snapshot ((store observed-event-store) stream-id)
  (%observed-call
   store
   :delete-snapshot
   (lambda ()
     (event-store-delete-snapshot
      (%observed-event-store-delegate store)
      stream-id))))

(defmethod event-store-snapshots-supported-p ((store observed-event-store))
  (%observed-call
   store
   :snapshots-supported-p
   (lambda ()
     (event-store-snapshots-supported-p
      (%observed-event-store-delegate store)))))

(defmethod event-store-prune ((store observed-event-store)
                              &key
                              (before-global-position *unspecified*))
  (%observed-call
   store
   :prune
   (lambda ()
     (event-store-prune
      (%observed-event-store-delegate store)
      :before-global-position before-global-position))))

(defmethod event-store-retention-supported-p ((store observed-event-store))
  (%observed-call
   store
   :retention-supported-p
   (lambda ()
     (event-store-retention-supported-p
      (%observed-event-store-delegate store)))))

(defmethod event-store-retention-floor ((store observed-event-store))
  (%observed-call
   store
   :retention-floor
   (lambda ()
     (event-store-retention-floor
      (%observed-event-store-delegate store)))))

(defmethod event-store-capabilities ((store observed-event-store))
  (event-store-capabilities (%observed-event-store-delegate store)))
