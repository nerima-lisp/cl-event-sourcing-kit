(defmethod event-store-read ((store in-memory-event-store)
                             stream-id
                             &key
                             from-version
                             to-version)
  "Read STREAM-ID in ascending inclusive version order.

FROM-VERSION and TO-VERSION are inclusive when supplied.  A missing stream
returns NIL, and omitted FROM-VERSION reads from the first version."
  (%ensure-in-memory-stream-id stream-id)
  (%validate-read-bound from-version)
  (%validate-read-bound to-version)
  (%with-in-memory-lock
   (store)
   (let ((events (gethash (%in-memory-identity-key stream-id)
                          (%in-memory-streams store)))
         (selected nil))
     (loop for event in events
           while (or (null from-version)
                     (>= (domain-event-version event) from-version))
           do (when (or
                      (null to-version)
                      (<= (domain-event-version event) to-version))
                (push event selected))
           finally (return selected)))))

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
     (let ((floor (%in-memory-global-position-floor store)))
       (when (< (or after-global-position 0)
                (max 0 (1- floor)))
         (error
          'event-store-retention-gap
          :store store
          :requested-position after-global-position
          :floor floor))
       (let ((cursor (or after-global-position 0)))
         (cond
           ((null limit)
            (let ((selected nil))
              (loop for event in (%in-memory-global-events store)
                    while (> (domain-event-global-position event) cursor)
                    do (push event selected))
              selected))
           ((zerop limit)
            nil)
           (t
            (let* ((events (%in-memory-global-ordered-events store))
                   (event-count (fill-pointer events)))
              (if (zerop event-count)
                  nil
                (let* ((first-position
                         (domain-event-global-position (aref events 0)))
                       (start-index
                         (max 0 (- (1+ cursor) first-position))))
                  (loop for index from start-index below event-count
                        while (< (- index start-index) limit)
                        collect (aref events index))))))))))))
