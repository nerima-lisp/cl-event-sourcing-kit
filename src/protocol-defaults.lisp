(defmethod event-store-append ((store event-store)
                               stream-id
                               events
                               &key
                               (expected-version *unspecified*))
  (declare (ignore stream-id events expected-version))
  (%signal-unsupported-event-store-operation store :append))

(defmethod event-store-read ((store event-store)
                             stream-id
                             &key
                             from-version
                             to-version)
  (declare (ignore stream-id from-version to-version))
  (%signal-unsupported-event-store-operation store :read))

(defmethod event-store-read-all ((store event-store)
                                 &key
                                 after-global-position
                                 limit)
  (declare (ignore after-global-position limit))
  (%signal-unsupported-event-store-operation store :read-all))

(defmethod event-store-current-version ((store event-store) stream-id)
  (declare (ignore stream-id))
  (%signal-unsupported-event-store-operation store :current-version))

(defmethod event-store-current-global-position ((store event-store))
  (%signal-unsupported-event-store-operation store :current-global-position))

(defmethod event-store-stream-exists-p ((store event-store) stream-id)
  (declare (ignore stream-id))
  (%signal-unsupported-event-store-operation store :stream-exists-p))

(defmethod event-store-global-position-supported-p ((store event-store))
  (declare (ignore store))
  nil)

(defmethod event-store-event-equivalent-p ((store event-store)
                                           existing-event
                                           requested-event)
  (declare (ignore store))
  (%domain-event-equivalent-p existing-event requested-event))

(defmethod event-store-append-batch ((store event-store) requests)
  (declare (ignore requests))
  (%signal-unsupported-event-store-operation store :append-batch))

(defmethod event-store-save-snapshot ((store event-store) snapshot)
  (declare (ignore snapshot))
  (%signal-unsupported-event-store-operation store :save-snapshot))

(defmethod event-store-read-snapshot ((store event-store)
                                      stream-id
                                      &key
                                      version)
  (declare (ignore stream-id version))
  (%signal-unsupported-event-store-operation store :read-snapshot))

(defmethod event-store-delete-snapshot ((store event-store) stream-id)
  (declare (ignore stream-id))
  (%signal-unsupported-event-store-operation store :delete-snapshot))

(defmethod event-store-snapshots-supported-p ((store event-store))
  (declare (ignore store))
  nil)

(defmethod event-store-prune ((store event-store)
                              &key before-global-position)
  (declare (ignore before-global-position))
  (%signal-unsupported-event-store-operation store :prune))

(defmethod event-store-retention-supported-p ((store event-store))
  (declare (ignore store))
  nil)

(defmethod event-store-retention-floor ((store event-store))
  (declare (ignore store))
  0)

(defmethod event-store-capabilities ((store t))
  (declare (ignore store))
  nil)
