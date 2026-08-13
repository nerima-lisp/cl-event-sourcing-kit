;;;; Durable projection checkpoints and restartable runners

(defun %copy-projection-checkpoint-record (record)
  (unless (projection-checkpoint-record-p record)
    (error 'type-error
           :datum record
           :expected-type 'projection-checkpoint-record))
  (make-projection-checkpoint-record
   :state (projection-checkpoint-record-state record)
   :position (projection-checkpoint-record-position record)
   :updated-at (projection-checkpoint-record-updated-at record)))

(defun %validate-projection-checkpoint-store-key (key)
  (%validate-durable-key
   key
   "A projection checkpoint requires a non-NIL key."))

(defun %validate-projection-checkpoint-record (record)
  (unless (projection-checkpoint-record-p record)
    (error 'type-error
           :datum record
           :expected-type 'projection-checkpoint-record))
  (unless (and (integerp (projection-checkpoint-record-position record))
               (<= 0 (projection-checkpoint-record-position record)))
    (error 'event-sourcing-error
           :message "A projection checkpoint position must be non-negative."))
  record)

(defun make-in-memory-projection-checkpoint-store (&key lock)
  (make-instance 'in-memory-projection-checkpoint-store
                 :checkpoints (make-hash-table :test #'equal)
                 :lock (or lock (%durable-lock "projection-checkpoints"))))

(defun make-file-projection-checkpoint-store (&key
                                               (path *unspecified*)
                                               serializer
                                               lock
                                               (sync #'finish-output))
  (%validate-durable-key
   path
   "A file projection checkpoint store requires a path.")
  (unless (functionp sync)
    (error 'type-error :datum sync :expected-type 'function))
  (make-instance 'file-projection-checkpoint-store
                 :path (%durable-pathname path)
                 :serializer (%resolve-event-serializer serializer)
                 :sync sync
                 :lock (or lock (%durable-lock "file-projection-checkpoints"))))

(defun %projection-checkpoint-wire (key record)
  (%validate-projection-checkpoint-store-key key)
  (%validate-projection-checkpoint-record record)
  (list :projection-checkpoint
        :key key
        :state (projection-checkpoint-record-state record)
        :position (projection-checkpoint-record-position record)
        :updated-at (projection-checkpoint-record-updated-at record)))

(defun %projection-checkpoints-wire (checkpoints)
  (let ((records nil))
    (maphash
     (lambda (key record)
       (push (%projection-checkpoint-wire key record) records))
     checkpoints)
    (cons :projection-checkpoints (nreverse records))))

(defun %projection-checkpoint-table-from-wire (wire path)
  (let ((checkpoints (make-hash-table :test #'equal)))
    (cond
      ((null wire) checkpoints)
      ((not (and (%proper-list-p wire)
                 (eq (first wire) :projection-checkpoints)))
       (error 'durable-store-corruption
              :path path
              :record wire
              :cause "The projection checkpoint file has an invalid envelope."))
      (t
       (dolist (record (rest wire))
         (unless (and (%proper-list-p record)
                      (eq (first record) :projection-checkpoint))
           (error 'durable-store-corruption
                  :path path
                  :record record
                  :cause "The projection checkpoint file has an invalid record."))
         (let* ((key (%required-wire-value record :key))
                (checkpoint
                  (make-projection-checkpoint-record
                   :state (%required-wire-value record :state)
                   :position (%required-wire-value record :position)
                   :updated-at (%required-wire-value record :updated-at))))
           (%validate-projection-checkpoint-store-key key)
           (when (nth-value 1 (gethash key checkpoints))
             (error 'durable-store-corruption
                    :path path
                    :record record
                    :cause "The projection checkpoint file contains a duplicate key."))
           (setf (gethash key checkpoints) checkpoint)))))
    checkpoints))

(defun %read-projection-checkpoints (store)
  (handler-case
      (%projection-checkpoint-table-from-wire
       (%read-serialized-file (%file-checkpoint-path store)
                              (%file-checkpoint-serializer store))
       (%file-checkpoint-path store))
    (durable-store-corruption (condition) (error condition))
    (error (cause)
      (error 'durable-store-corruption
             :path (%file-checkpoint-path store)
             :cause cause))))

(defun %write-projection-checkpoints (store checkpoints)
  (%write-serialized-file (%file-checkpoint-path store)
                          (%projection-checkpoints-wire checkpoints)
                          (%file-checkpoint-serializer store)
                          (%file-checkpoint-sync store)))

(defmethod projection-checkpoint-load
    ((store in-memory-projection-checkpoint-store) key)
  (%validate-projection-checkpoint-store-key key)
  (%with-durable-lock ((%in-memory-checkpoint-lock store))
    (let ((record (gethash key (%in-memory-checkpoints store))))
      (and record (%copy-projection-checkpoint-record record)))))

(defmethod projection-checkpoint-save
    ((store in-memory-projection-checkpoint-store) key record)
  (%validate-projection-checkpoint-store-key key)
  (%validate-projection-checkpoint-record record)
  (%with-durable-lock ((%in-memory-checkpoint-lock store))
    (let ((old (gethash key (%in-memory-checkpoints store))))
      (when (and old
                 (> (projection-checkpoint-record-position old)
                    (projection-checkpoint-record-position record)))
        (error 'event-sourcing-error
               :message "A projection checkpoint cannot move backwards."))
      (setf (gethash key (%in-memory-checkpoints store))
            (%copy-projection-checkpoint-record record))
      (%copy-projection-checkpoint-record record))))

(defmethod projection-checkpoint-delete
    ((store in-memory-projection-checkpoint-store) key)
  (%validate-projection-checkpoint-store-key key)
  (%with-durable-lock ((%in-memory-checkpoint-lock store))
    (remhash key (%in-memory-checkpoints store))))

(defmethod projection-checkpoint-load
    ((store file-projection-checkpoint-store) key)
  (%validate-projection-checkpoint-store-key key)
  (%with-durable-lock ((%file-checkpoint-lock store))
    (let ((record (gethash key (%read-projection-checkpoints store))))
      (and record (%copy-projection-checkpoint-record record)))))

(defmethod projection-checkpoint-save
    ((store file-projection-checkpoint-store) key record)
  (%validate-projection-checkpoint-store-key key)
  (%validate-projection-checkpoint-record record)
  (%with-durable-lock ((%file-checkpoint-lock store))
    (let* ((checkpoints (%read-projection-checkpoints store))
           (old (gethash key checkpoints)))
      (when (and old
                 (> (projection-checkpoint-record-position old)
                    (projection-checkpoint-record-position record)))
        (error 'event-sourcing-error
               :message "A projection checkpoint cannot move backwards."))
      (setf (gethash key checkpoints)
            (%copy-projection-checkpoint-record record))
      (%write-projection-checkpoints store checkpoints)
      (%copy-projection-checkpoint-record record))))

(defmethod projection-checkpoint-delete
    ((store file-projection-checkpoint-store) key)
  (%validate-projection-checkpoint-store-key key)
  (%with-durable-lock ((%file-checkpoint-lock store))
    (let ((checkpoints (%read-projection-checkpoints store)))
      (if (nth-value 1 (gethash key checkpoints))
          (progn
            (remhash key checkpoints)
            (%write-projection-checkpoints store checkpoints)
            t)
        nil))))

(defun make-durable-projection-runner (&key
                                        (projection *unspecified*)
                                        (event-store *unspecified*)
                                        (checkpoint-store *unspecified*)
                                        (checkpoint-key *unspecified*)
                                        lock)
  (unless (projection-p projection)
    (error 'type-error :datum projection :expected-type 'projection))
  (unless (projection-checkpoint-store-p checkpoint-store)
    (error 'type-error
           :datum checkpoint-store
           :expected-type 'projection-checkpoint-store))
  (%validate-projection-checkpoint-store-key checkpoint-key)
  (unless (event-store-global-position-supported-p event-store)
    (error 'event-store-operation-not-supported
           :operation 'make-durable-projection-runner
           :store event-store))
  (let ((runner
          (make-instance 'durable-projection-runner
                         :projection projection
                         :event-store event-store
                         :checkpoint-store checkpoint-store
                         :checkpoint-key checkpoint-key
                         :lock (or lock (%durable-lock "projection-runner")))))
    (unless (typep (%durable-runner-lock runner)
                   'cl-concurrent-kit:lock)
      (error 'type-error
             :datum (%durable-runner-lock runner)
             :expected-type 'cl-concurrent-kit:lock))
    (let ((record (projection-checkpoint-load checkpoint-store checkpoint-key)))
      (if record
          (setf (slot-value projection 'state)
                (projection-checkpoint-record-state record)
                (slot-value projection 'checkpoint)
                (projection-checkpoint-record-position record))
        (projection-checkpoint-save
         checkpoint-store
         checkpoint-key
         (make-projection-checkpoint-record
          :state (projection-state projection)
          :position (projection-checkpoint projection)
          :updated-at (get-universal-time)))))
    runner))

(defun %durable-runner-record (runner)
  (make-projection-checkpoint-record
   :state (projection-state (durable-projection-runner-projection runner))
   :position (projection-checkpoint
              (durable-projection-runner-projection runner))
   :updated-at (get-universal-time)))

(defmethod run-projection-once ((runner durable-projection-runner) &key limit)
  (%with-durable-lock ((%durable-runner-lock runner))
    (let* ((projection (durable-projection-runner-projection runner))
           (event-store (durable-projection-runner-event-store runner))
           (checkpoint-store
             (durable-projection-runner-checkpoint-store runner))
           (key (%durable-runner-key runner))
           (saved (projection-checkpoint-load checkpoint-store key)))
      (when saved
        (setf (slot-value projection 'state)
              (projection-checkpoint-record-state saved)
              (slot-value projection 'checkpoint)
              (projection-checkpoint-record-position saved)))
      (let ((before-state (projection-state projection))
            (before-checkpoint (projection-checkpoint projection)))
        (handler-case
            (multiple-value-bind (state checkpoint)
                (advance-projection projection event-store :limit limit)
              (handler-case
                  (progn
                    (projection-checkpoint-save
                     checkpoint-store
                     key
                     (%durable-runner-record runner))
                    (values state checkpoint))
                (error (cause)
                  (setf (slot-value projection 'state) before-state
                        (slot-value projection 'checkpoint) before-checkpoint)
                  (error cause))))
          (error (cause)
            (setf (slot-value projection 'state) before-state
                  (slot-value projection 'checkpoint) before-checkpoint)
            (error cause)))))))
