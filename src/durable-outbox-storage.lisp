;;;; Transactional application outbox

(defun %validate-outbox-message (message)
  (unless (outbox-message-p message)
    (error 'type-error :datum message :expected-type 'outbox-message))
  (when (or (null (outbox-message-id message))
            (eq (outbox-message-id message) *unspecified*))
    (error 'event-sourcing-error
           :message "An outbox message requires a non-NIL id."))
  (when (or (null (outbox-message-topic message))
            (eq (outbox-message-topic message) *unspecified*))
    (error 'event-sourcing-error
           :message "An outbox message requires a non-NIL topic."))
  (unless (and (member (outbox-message-status message)
                       '(:pending :in-flight :delivered :dead-letter)
                       :test #'eq)
               (integerp (outbox-message-attempts message))
               (<= 0 (outbox-message-attempts message)))
    (error 'event-sourcing-error
           :message "An outbox message has invalid delivery state."))
  (unless (or (null (outbox-message-last-error message))
              (stringp (outbox-message-last-error message)))
    (error 'type-error
           :datum (outbox-message-last-error message)
           :expected-type '(or null string)))
  (unless (or (null (outbox-message-dead-lettered-at message))
              (realp (outbox-message-dead-lettered-at message)))
    (error 'type-error
           :datum (outbox-message-dead-lettered-at message)
           :expected-type '(or null real)))
  message)

(defun %copy-outbox-message (message &key
                                       (status *unspecified*)
                                       (attempts *unspecified*)
                                       (claimed-at *unspecified*)
                                       (available-at *unspecified*)
                                       (claim-token *unspecified*)
                                       (last-error *unspecified*)
                                       (dead-lettered-at *unspecified*))
  (%make-outbox-message
   (outbox-message-id message)
   (outbox-message-topic message)
   (outbox-message-payload message)
   (outbox-message-metadata message)
   (outbox-message-created-at message)
   (if (eq status *unspecified*) (outbox-message-status message) status)
   (if (eq attempts *unspecified*) (outbox-message-attempts message) attempts)
   (if (eq claimed-at *unspecified*)
       (outbox-message-claimed-at message)
     claimed-at)
   (if (eq available-at *unspecified*)
       (outbox-message-available-at message)
     available-at)
   (if (eq claim-token *unspecified*)
       (outbox-message-claim-token message)
     claim-token)
   (if (eq last-error *unspecified*)
       (outbox-message-last-error message)
     last-error)
   (if (eq dead-lettered-at *unspecified*)
       (outbox-message-dead-lettered-at message)
     dead-lettered-at)))

(defun %replace-outbox-message-state (target replacement)
  (setf (outbox-message-status target)
        (outbox-message-status replacement)
        (outbox-message-attempts target)
        (outbox-message-attempts replacement)
        (outbox-message-claimed-at target)
        (outbox-message-claimed-at replacement)
        (outbox-message-available-at target)
        (outbox-message-available-at replacement)
        (outbox-message-claim-token target)
        (outbox-message-claim-token replacement)
        (outbox-message-last-error target)
        (outbox-message-last-error replacement)
        (outbox-message-dead-lettered-at target)
        (outbox-message-dead-lettered-at replacement))
  target)

(defun %outbox-message-equivalent-p (existing requested)
  (and
   (%safe-equal-p (outbox-message-id existing)
                  (outbox-message-id requested))
   (%safe-equal-p (outbox-message-topic existing)
                  (outbox-message-topic requested))
   (%safe-equal-p (outbox-message-payload existing)
                  (outbox-message-payload requested))
   (%safe-equal-p (outbox-message-metadata existing)
                  (outbox-message-metadata requested))
   (%safe-equal-p (outbox-message-created-at existing)
                  (outbox-message-created-at requested))))

(defun %outbox-find (messages message-id &optional message-index)
  (if message-index
      (gethash message-id message-index)
    (find message-id messages
          :test (lambda (requested existing)
                  (%safe-equal-p requested (outbox-message-id existing))))))

(defun %make-outbox-message-index (messages)
  (let ((index (make-hash-table :test #'equal)))
    (dolist (message messages index)
      (setf (gethash (outbox-message-id message) index) message))))

(defun %make-outbox-message-order (messages)
  (let ((order (make-array (max 16 (length messages))
                           :adjustable t
                           :fill-pointer 0)))
    (dolist (message messages order)
      (vector-push-extend message order (max 64 (array-total-size order))))))

(defun %copy-outbox-message-order (messages)
  (let ((copy (make-array (max 16 (length messages))
                          :adjustable t
                          :fill-pointer 0)))
    (loop for message across messages
          do (vector-push-extend
              message copy (max 64 (array-total-size copy))))
    copy))

(defun %signal-outbox-conflict (existing requested)
  (error 'outbox-message-conflict
         :message-id (outbox-message-id requested)
         :existing-message existing
         :requested-message requested))

(defun %validate-outbox-message-list (messages)
  (unless (%proper-list-p messages)
    (error 'type-error :datum messages :expected-type 'list))
  (let ((ids (make-hash-table :test #'equal))
        (result nil))
    (dolist (message messages)
      (%validate-outbox-message message)
      (let ((id (outbox-message-id message)))
        (when (gethash id ids)
          (%signal-outbox-conflict (gethash id ids) message))
        (setf (gethash id ids) message))
      (push (%copy-outbox-message message) result))
    (nreverse result)))

(defun make-in-memory-outbox-store (&key
                                      (messages *unspecified* messages-supplied-p)
                                      lock)
  (unless messages-supplied-p
    (setf messages nil))
  (let* ((validated-messages (%validate-outbox-message-list messages))
         (stored-messages (nreverse (copy-list validated-messages)))
         (resolved-lock (or lock (%durable-lock "cl-event-sourcing-kit/outbox"))))
    (unless (typep resolved-lock 'cl-concurrent-kit:lock)
      (error 'type-error
             :datum resolved-lock
             :expected-type 'cl-concurrent-kit:lock))
    (make-instance 'in-memory-outbox-store
                   :messages stored-messages
                   :ordered-messages (%make-outbox-message-order
                                      validated-messages)
                   :message-index (%make-outbox-message-index stored-messages)
                   :lock resolved-lock)))

(defun %outbox-wire (message)
  (%validate-outbox-message message)
  (list :outbox-message
        :id (outbox-message-id message)
        :topic (outbox-message-topic message)
        :payload (outbox-message-payload message)
        :metadata (outbox-message-metadata message)
        :created-at (outbox-message-created-at message)
        :status (outbox-message-status message)
        :attempts (outbox-message-attempts message)
        :claimed-at (outbox-message-claimed-at message)
        :available-at (outbox-message-available-at message)
        :claim-token (outbox-message-claim-token message)
        :last-error (outbox-message-last-error message)
        :dead-lettered-at (outbox-message-dead-lettered-at message)))

(defun %outbox-from-wire (wire)
  (unless (and (%proper-list-p wire) (eq (first wire) :outbox-message))
    (error "Serialized value is not an outbox-message record."))
  (make-outbox-message
   :id (%required-wire-value wire :id)
   :topic (%required-wire-value wire :topic)
   :payload (%required-wire-value wire :payload)
   :metadata (%required-wire-value wire :metadata)
   :created-at (%required-wire-value wire :created-at)
   :status (%required-wire-value wire :status)
   :attempts (%required-wire-value wire :attempts)
   :claimed-at (%required-wire-value wire :claimed-at)
   :available-at (%required-wire-value wire :available-at)
   :claim-token (%required-wire-value wire :claim-token)
   :last-error (%required-wire-value wire :last-error)
   :dead-lettered-at (%required-wire-value wire :dead-lettered-at)))

(defun %outbox-messages-from-wire (wire path)
  (handler-case
      (progn
        (unless (and (%proper-list-p wire) (eq (first wire) :outbox-store))
          (error "An outbox file has an invalid envelope."))
        (let ((messages (%required-wire-value wire :messages)))
          (unless (%proper-list-p messages)
            (error "An outbox file has an invalid message list."))
          (%validate-outbox-message-list
           (mapcar #'%outbox-from-wire messages))))
    (error (cause)
      (error 'durable-store-corruption
             :path path
             :record wire
             :cause cause))))

(defun %read-outbox-messages (store)
  (let ((path (file-outbox-store-path store)))
    (if (not (probe-file path))
        nil
      (%outbox-messages-from-wire
       (%read-serialized-file path (%file-outbox-serializer store))
       path))))

(defun %write-outbox-messages (store messages)
  (%write-serialized-file
   (file-outbox-store-path store)
   (list :outbox-store :messages (mapcar #'%outbox-wire messages))
   (%file-outbox-serializer store)
   (%file-outbox-sync store)))

(defun make-file-outbox-store (&key
                                (path *unspecified*)
                                serializer
                                lock
                                (sync #'finish-output))
  (when (eq path *unspecified*)
    (error 'event-sourcing-error :message "A file outbox requires PATH."))
  (unless (functionp sync)
    (error 'type-error :datum sync :expected-type 'function))
  (let ((resolved-lock (or lock (%durable-lock "cl-event-sourcing-kit/file-outbox"))))
    (unless (typep resolved-lock 'cl-concurrent-kit:lock)
      (error 'type-error
             :datum resolved-lock
             :expected-type 'cl-concurrent-kit:lock))
    (let ((store (make-instance
                  'file-outbox-store
                  :path (%durable-pathname path)
                  :serializer (%resolve-event-serializer serializer)
                  :sync sync
                  :lock resolved-lock)))
      ;; Reading here validates an existing file before the object escapes.
      (%with-durable-lock ((%file-outbox-lock store))
        (%read-outbox-messages store))
      store)))

(defun %outbox-append-to-list (messages message)
  (%validate-outbox-message message)
  (let ((existing (%outbox-find messages (outbox-message-id message))))
    (if existing
        (if (%outbox-message-equivalent-p existing message)
            (values messages (%copy-outbox-message existing))
          (%signal-outbox-conflict existing message))
      (values (append messages (list (%copy-outbox-message message)))
              (%copy-outbox-message message)))))

(defmethod outbox-append ((store in-memory-outbox-store) message)
  (%with-durable-lock ((%in-memory-outbox-lock store))
    (%validate-outbox-message message)
    (let* ((message-id (outbox-message-id message))
           (message-index (%in-memory-outbox-index store))
           (existing (gethash message-id message-index)))
      (if existing
          (if (%outbox-message-equivalent-p existing message)
              (%copy-outbox-message existing)
            (%signal-outbox-conflict existing message))
        (let ((copy (%copy-outbox-message message)))
          (vector-push-extend
           copy
           (%in-memory-outbox-ordered-messages store)
           (max 64
                (array-total-size
                 (%in-memory-outbox-ordered-messages store))))
          (setf (slot-value store 'messages)
                (cons copy (%in-memory-outbox-messages store))
                (gethash message-id message-index)
                copy)
          copy)))))

(defmethod outbox-append ((store file-outbox-store) message)
  (%with-durable-lock ((%file-outbox-lock store))
    (let ((messages (%read-outbox-messages store)))
      (multiple-value-bind (next result)
          (%outbox-append-to-list messages message)
        (unless (eq next messages)
          (%write-outbox-messages store next))
        result))))
