(defmethod event-store-append ((store t)
                               stream-id
                               events
                               &key
                               (expected-version *unspecified*))
  (declare (ignore stream-id events expected-version))
  (%signal-unsupported-event-store-operation store :append))

(defmethod event-store-read ((store t)
                             stream-id
                             &key
                             from-version
                             to-version)
  (declare (ignore stream-id from-version to-version))
  (%signal-unsupported-event-store-operation store :read))

(defmethod event-store-read-all ((store t)
                                 &key
                                 after-global-position
                                 limit)
  (declare (ignore after-global-position limit))
  (%signal-unsupported-event-store-operation store :read-all))

(defmethod event-store-current-version ((store t) stream-id)
  (declare (ignore stream-id))
  (%signal-unsupported-event-store-operation store :current-version))

(defmethod event-store-current-global-position ((store t))
  (%signal-unsupported-event-store-operation store :current-global-position))

(defmethod event-store-stream-exists-p ((store t) stream-id)
  (declare (ignore stream-id))
  (%signal-unsupported-event-store-operation store :stream-exists-p))

(defmethod event-store-global-position-supported-p ((store t))
  (declare (ignore store))
  nil)

(defmethod event-store-event-equivalent-p ((store t)
                                           existing-event
                                           requested-event)
  (declare (ignore store))
  (%domain-event-equivalent-p existing-event requested-event))

(defmethod event-store-append-batch ((store t) requests)
  (declare (ignore requests))
  (%signal-unsupported-event-store-operation store :append-batch))

(defmethod event-store-save-snapshot ((store t) snapshot)
  (declare (ignore snapshot))
  (%signal-unsupported-event-store-operation store :save-snapshot))

(defmethod event-store-read-snapshot ((store t)
                                      stream-id
                                      &key
                                      version)
  (declare (ignore stream-id version))
  (%signal-unsupported-event-store-operation store :read-snapshot))

(defmethod event-store-delete-snapshot ((store t) stream-id)
  (declare (ignore stream-id))
  (%signal-unsupported-event-store-operation store :delete-snapshot))

(defmethod event-store-snapshots-supported-p ((store t))
  (declare (ignore store))
  nil)

(defmethod event-store-prune ((store t)
                              &key before-global-position)
  (declare (ignore before-global-position))
  (%signal-unsupported-event-store-operation store :prune))

(defmethod event-store-retention-supported-p ((store t))
  (declare (ignore store))
  nil)

(defmethod event-store-retention-floor ((store t))
  (declare (ignore store))
  0)

(defmethod event-store-capabilities ((store t))
  (declare (ignore store))
  nil)
