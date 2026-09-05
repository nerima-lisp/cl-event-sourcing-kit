;;;; Package selection is a reader-time declaration.  Keeping it out of the
;;;; compiled form lets SB-COVER measure the snapshot operations below without
;;;; counting an uncallable load-time form as runtime behavior.
#.(progn (in-package #:cl-event-sourcing-kit) nil)

(defun %validate-snapshot (snapshot)
  (%validate-event-snapshot snapshot))

(defun %snapshot-history-with (snapshot history)
  (sort
   (cons snapshot
         (remove-if
          (lambda (existing)
            (= (event-snapshot-version existing)
               (event-snapshot-version snapshot)))
          (copy-list history)))
   #'>
   :key
   #'event-snapshot-version))

(defmethod event-store-save-snapshot ((store in-memory-event-store) snapshot)
  "Save a snapshot and retain historical versions for bounded reads.

Snapshots at version zero are allowed for an as-yet empty stream.  A snapshot
cannot describe a future version.  Saving the same version replaces its
previous snapshot."
  (%validate-snapshot snapshot)
  (let* ((stream-id (event-snapshot-stream-id snapshot))
         (canonical-stream-id (%in-memory-identity-key stream-id))
         (version (event-snapshot-version snapshot)))
    (%with-in-memory-lock
     (store)
     (multiple-value-bind (actual-version exists-p)
         (%in-memory-current-version-locked store canonical-stream-id)
       (when (and (not exists-p) (plusp version))
         (error
          'invalid-snapshot
          :stream-id stream-id
          :version version
          :reason :missing-stream))
       (when (> version actual-version)
         (error
          'invalid-snapshot
          :stream-id stream-id
          :version version
          :reason :future-version))
       (let ((canonical-snapshot
               (make-event-snapshot
                :stream-id canonical-stream-id
                :version version
                :state (event-snapshot-state snapshot)
                :metadata (event-snapshot-metadata snapshot)
                :timestamp (event-snapshot-timestamp snapshot))))
         (setf (gethash canonical-stream-id (%in-memory-snapshots store))
               (%snapshot-history-with
                canonical-snapshot
                (gethash canonical-stream-id (%in-memory-snapshots store))))
         canonical-snapshot)))))

(defmethod event-store-read-snapshot ((store in-memory-event-store)
                                      stream-id
                                      &key
                                      version)
  "Read the latest snapshot whose version is not newer than VERSION."
  (%ensure-in-memory-stream-id stream-id)
  (%validate-read-bound version)
  (%with-in-memory-lock
   (store)
   (find-if
    (lambda (snapshot)
      (or (null version)
          (<= (event-snapshot-version snapshot) version)))
    (gethash (%in-memory-identity-key stream-id)
             (%in-memory-snapshots store)))))

(defmethod event-store-delete-snapshot ((store in-memory-event-store) stream-id)
  (%ensure-in-memory-stream-id stream-id)
  (%with-in-memory-lock
   (store)
   (remhash (%in-memory-identity-key stream-id)
            (%in-memory-snapshots store))))

(defmethod event-store-snapshots-supported-p ((store in-memory-event-store))
  (declare (ignore store))
  t)
