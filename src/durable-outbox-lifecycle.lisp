(defun %claim-token-matches-p (message claim-token)
  (or (eq claim-token *unspecified*)
      (%safe-equal-p claim-token (outbox-message-claim-token message))))

(defun %outbox-ack-list
    (messages message-id claim-token &optional message-index in-place-p)
  (let ((existing (%outbox-find messages message-id message-index)))
    (unless existing
      (return-from %outbox-ack-list (values messages nil)))
    (unless (%claim-token-matches-p existing claim-token)
      (error 'event-sourcing-error
             :message "An outbox acknowledgement has a stale claim token."))
    (when (eq (outbox-message-status existing) :dead-letter)
      (error 'event-sourcing-error
             :message "A dead-letter outbox message must be requeued before acknowledgement."))
    (when (and (eq (outbox-message-status existing) :pending)
               (not (eq claim-token *unspecified*)))
      (error 'event-sourcing-error
             :message "A pending outbox message cannot be acknowledged with a claim token."))
    (if (eq (outbox-message-status existing) :delivered)
        (values messages (%copy-outbox-message existing))
      (let ((replacement (%copy-outbox-message existing
                                                :status :delivered
                                                :claimed-at nil
                                                :claim-token nil)))
        (if in-place-p
            (progn
              (%replace-outbox-message-state existing replacement)
              (values messages (%copy-outbox-message existing)))
          (values (mapcar (lambda (message)
                            (if (eq message existing) replacement message))
                          messages)
                  replacement))))))

(defmethod outbox-ack ((store in-memory-outbox-store)
                       message-id
                       &key (claim-token *unspecified*))
  (%with-durable-lock ((%in-memory-outbox-lock store))
    (nth-value 1
               (%outbox-ack-list (%in-memory-outbox-messages store)
                                 message-id
                                 claim-token
                                 (%in-memory-outbox-index store)
                                 t))))

(defmethod outbox-ack ((store file-outbox-store)
                       message-id
                       &key (claim-token *unspecified*))
  (%with-durable-lock ((%file-outbox-lock store))
    (let ((old (%read-outbox-messages store)))
      (multiple-value-bind (messages result)
          (%outbox-ack-list old message-id claim-token)
        (unless (eq messages old) (%write-outbox-messages store messages))
        result))))

(defun %outbox-failure-value (existing failure)
  (cond
    ((eq failure *unspecified*) (outbox-message-last-error existing))
    ((null failure) nil)
    (t (princ-to-string failure))))

(defun %outbox-fail-list
    (messages message-id now backoff-seconds claim-token failure max-attempts
              &optional message-index in-place-p)
  (let ((existing (%outbox-find messages message-id message-index)))
    (unless existing
      (return-from %outbox-fail-list (values messages nil nil)))
    (unless (%claim-token-matches-p existing claim-token)
      (error 'event-sourcing-error
             :message "An outbox failure has a stale claim token."))
    (when (eq (outbox-message-status existing) :dead-letter)
      (error 'event-sourcing-error
             :message "A dead-letter outbox message must be requeued before failure."))
    (let* ((dead-lettered-p (and max-attempts
                                 (>= (outbox-message-attempts existing)
                                     max-attempts)))
           (replacement
             (%copy-outbox-message
              existing
              :status (if dead-lettered-p :dead-letter :pending)
              :claimed-at nil
              :available-at (unless dead-lettered-p
                              (+ now backoff-seconds))
              :claim-token nil
              :last-error (%outbox-failure-value existing failure)
              :dead-lettered-at (and dead-lettered-p now))))
      (if in-place-p
          (progn
            (%replace-outbox-message-state existing replacement)
            (values messages (%copy-outbox-message existing) dead-lettered-p))
        (values (mapcar (lambda (message)
                          (if (eq message existing) replacement message))
                        messages)
                replacement
                dead-lettered-p)))))

(defmethod outbox-fail ((store in-memory-outbox-store)
                        message-id
                        &key
                        (now *unspecified*)
                        (backoff-seconds *unspecified* backoff-seconds-supplied-p)
                        (claim-token *unspecified*)
                        (failure *unspecified*)
                        max-attempts)
  (unless backoff-seconds-supplied-p
    (setf backoff-seconds 0))
  (let ((now (%outbox-now now)))
    (%validate-outbox-time now)
    (%validate-outbox-lease backoff-seconds)
    (%validate-outbox-max-attempts max-attempts)
    (%with-durable-lock ((%in-memory-outbox-lock store))
      (multiple-value-bind (messages result dead-lettered-p)
          (%outbox-fail-list (%in-memory-outbox-messages store)
                             message-id now backoff-seconds claim-token
                             failure max-attempts
                             (%in-memory-outbox-index store)
                             t)
        (declare (ignore messages))
        (values result dead-lettered-p)))))

(defmethod outbox-fail ((store file-outbox-store)
                        message-id
                        &key
                        (now *unspecified*)
                        (backoff-seconds *unspecified* backoff-seconds-supplied-p)
                        (claim-token *unspecified*)
                        (failure *unspecified*)
                        max-attempts)
  (unless backoff-seconds-supplied-p
    (setf backoff-seconds 0))
  (let ((now (%outbox-now now)))
    (%validate-outbox-time now)
    (%validate-outbox-lease backoff-seconds)
    (%validate-outbox-max-attempts max-attempts)
    (%with-durable-lock ((%file-outbox-lock store))
      (let ((old (%read-outbox-messages store)))
        (multiple-value-bind (messages result dead-lettered-p)
            (%outbox-fail-list old message-id now backoff-seconds claim-token
                               failure max-attempts)
          (unless (eq messages old) (%write-outbox-messages store messages))
          (values result dead-lettered-p))))))

(defun %outbox-requeue-list
    (messages message-id available-at &optional message-index in-place-p)
  (let ((existing (%outbox-find messages message-id message-index)))
    (unless existing
      (return-from %outbox-requeue-list (values messages nil)))
    (case (outbox-message-status existing)
      (:dead-letter
       (let ((replacement
               (%copy-outbox-message existing
                                     :status :pending
                                     :claimed-at nil
                                     :available-at available-at
                                     :claim-token nil
                                     :last-error nil
                                     :dead-lettered-at nil)))
         (if in-place-p
             (progn
               (%replace-outbox-message-state existing replacement)
               (values messages (%copy-outbox-message existing)))
           (values (mapcar (lambda (message)
                             (if (eq message existing) replacement message))
                         messages)
                   replacement))))
      (:pending (values messages (%copy-outbox-message existing)))
      (otherwise
       (error 'event-sourcing-error
              :message "Only a dead-letter or pending outbox message can be requeued.")))))

(defmethod outbox-requeue ((store in-memory-outbox-store)
                           message-id
                           &key (available-at *unspecified*))
  (let ((available-at (if (eq available-at *unspecified*)
                          (get-universal-time)
                        available-at)))
    (%validate-outbox-time available-at)
    (%with-durable-lock ((%in-memory-outbox-lock store))
      (nth-value 1
                 (%outbox-requeue-list (%in-memory-outbox-messages store)
                                       message-id
                                       available-at
                                       (%in-memory-outbox-index store)
                                       t)))))

(defmethod outbox-requeue ((store file-outbox-store)
                           message-id
                           &key (available-at *unspecified*))
  (let ((available-at (if (eq available-at *unspecified*)
                          (get-universal-time)
                        available-at)))
    (%validate-outbox-time available-at)
    (%with-durable-lock ((%file-outbox-lock store))
      (let ((old (%read-outbox-messages store)))
        (multiple-value-bind (messages result)
            (%outbox-requeue-list old message-id available-at)
          (unless (eq messages old) (%write-outbox-messages store messages))
          result)))))

(defmethod outbox-dispatch ((store outbox-store) handler
                            &key
                            (now *unspecified*)
                            limit
                            (lease-seconds *unspecified* lease-seconds-supplied-p)
                            (backoff-seconds *unspecified* backoff-seconds-supplied-p)
                            max-attempts)
  (unless lease-seconds-supplied-p
    (setf lease-seconds 60))
  (unless backoff-seconds-supplied-p
    (setf backoff-seconds 0))
  (unless (functionp handler)
    (error 'type-error :datum handler :expected-type 'function))
  (%validate-outbox-max-attempts max-attempts)
  (let ((claimed (outbox-claim store
                               :now now
                               :limit limit
                               :lease-seconds lease-seconds))
        (delivered 0)
        (dead-lettered 0))
    (dolist (message claimed)
      (handler-case
          (progn
            (funcall handler message)
            (outbox-ack store
                        (outbox-message-id message)
                        :claim-token (outbox-message-claim-token message))
            (incf delivered))
        (error (cause)
          (multiple-value-bind (ignored dead-lettered-p)
              (outbox-fail store
                           (outbox-message-id message)
                           :now (%outbox-now now)
                           :backoff-seconds backoff-seconds
                           :claim-token (outbox-message-claim-token message)
                           :failure cause
                           :max-attempts max-attempts)
            (declare (ignore ignored))
            (when dead-lettered-p (incf dead-lettered))))))
    (values delivered (length claimed) dead-lettered)))
