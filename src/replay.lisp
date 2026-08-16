(defun replay-events (initial-state events reducer &key upcaster)
  "Fold EVENTS from left to right with REDUCER.

The reducer receives STATE and one immutable DOMAIN-EVENT and returns the next
state.  UPCASTER may transform stored event schemas before the reducer sees
them.  No sorting, persistence, serialization, or mutation of EVENTS is
performed."
  (unless (functionp reducer)
    (error 'type-error :datum reducer :expected-type 'function))
  (let ((state initial-state))
    (dolist (event (%coerce-event-list events) state)
      (setf state
            (funcall reducer state (upcast-event event upcaster))))))

(defun %validate-committed-event (event)
  (unless (domain-event-p event)
    (%invalid-domain-event event :event-type))
  (unless (typep (domain-event-version event) '(integer 0 *))
    (%invalid-domain-event event :version))
  event)

(defun %validate-aggregate-event (event stream-id)
  (let ((event (%validate-committed-event event)))
    (if (%safe-equal-p stream-id (domain-event-stream-id event))
        event
        (%invalid-domain-event
         event
         :stream-id
         "The event belongs to a different aggregate stream."))))

(defun load-aggregate (store stream-id initial-state reducer
                       &key
                       (use-snapshot *unspecified*)
                       upcaster)
  "Load one aggregate, optionally starting from its newest snapshot.

Snapshots are an optimization boundary: the reducer receives the snapshot
state and only events after the snapshot version.  The store remains
responsible for making a snapshot durable and consistent with its event log;
the core validates the value returned by the adapter before using it.  The
first return value is the aggregate state and the second is its last stream
version (zero for an empty stream)."
  (when (eq use-snapshot *unspecified*)
    (setf use-snapshot t))
  (unless (or (null use-snapshot) (eq use-snapshot t))
    (error 'type-error :datum use-snapshot :expected-type 'boolean))
  (let ((state initial-state)
        (from-version nil)
        (last-version 0))
    (when (and use-snapshot
               (event-store-snapshots-supported-p store))
      (let ((snapshot (event-store-read-snapshot store stream-id)))
        (when snapshot
          (%validate-event-snapshot snapshot :stream-id stream-id)
          (setf state (event-snapshot-state snapshot)
                from-version (1+ (event-snapshot-version snapshot))
                last-version (event-snapshot-version snapshot)))))
    (let ((events
            (mapcar
             (lambda (event)
               (let ((validated
                       (%validate-aggregate-event event stream-id)))
                 (%validate-aggregate-event
                  (upcast-event validated upcaster)
                  stream-id)))
             (%coerce-event-list
              (event-store-read
               store
               stream-id
               :from-version
               from-version)))))
      (dolist (event events)
        (let ((version (domain-event-version event)))
          (unless (= version (1+ last-version))
            (%invalid-domain-event
             event
             :version-order
             "The aggregate event versions are not contiguous."))
          (setf last-version version)))
      (values
       (replay-events state events reducer)
       last-version))))
