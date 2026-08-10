(defun %outbox-now (now)
  (if (eq now *unspecified*) (get-universal-time) now))

(defun %validate-outbox-time (now)
  (unless (realp now)
    (error 'type-error :datum now :expected-type 'real))
  now)

(defun %validate-outbox-limit (limit)
  (when (and limit (not (and (integerp limit) (plusp limit))))
    (error 'type-error :datum limit :expected-type '(or null (integer 1 *))))
  limit)

(defun %validate-outbox-status (status)
  (when (and (not (eq status *unspecified*))
             (not (member status '(:pending :in-flight :delivered :dead-letter)
                            :test #'eq)))
    (error 'type-error :datum status :expected-type 'member))
  status)

(defun %validate-outbox-max-attempts (max-attempts)
  (when (and max-attempts
             (not (and (integerp max-attempts) (plusp max-attempts))))
    (error 'type-error
           :datum max-attempts
           :expected-type '(or null (integer 1 *))))
  max-attempts)

(defun %validate-outbox-lease (seconds)
  (unless (and (realp seconds) (<= 0 seconds))
    (error 'type-error :datum seconds :expected-type '(real 0 *)))
  seconds)

(defun %outbox-available-p (message now)
  (let ((available-at (outbox-message-available-at message)))
    (or (null available-at)
        (and (realp available-at) (<= available-at now)))))

(defun %outbox-status-matches-p (message status)
  (or (eq status *unspecified*)
      (eq (outbox-message-status message) status)))

(defun %select-outbox-messages (messages status limit)
  (let ((selected
          (remove-if-not
           (lambda (message)
             (%outbox-status-matches-p message status))
           messages)))
    (when limit
      (setf selected (subseq selected 0 (min limit (length selected)))))
    (mapcar #'%copy-outbox-message selected)))

(defun %select-in-memory-outbox-messages (messages status limit)
  (let ((selected nil)
        (count 0))
    (loop for message across messages
          while (or (null limit) (< count limit))
          when (%outbox-status-matches-p message status)
            do (push (%copy-outbox-message message) selected)
               (incf count))
    (nreverse selected)))

(defun %outbox-expired-p (message now lease-seconds)
  (and (eq (outbox-message-status message) :in-flight)
       (realp (outbox-message-claimed-at message))
       (<= (+ (outbox-message-claimed-at message) lease-seconds) now)))

(defun %pending-outbox-messages (messages now)
  (remove-if-not
   (lambda (message)
     (and (eq (outbox-message-status message) :pending)
          (%outbox-available-p message now)))
   messages))

(defun %pending-in-memory-outbox-messages (messages now limit)
  (let ((selected nil)
        (count 0))
    (loop for message across messages
          while (or (null limit) (< count limit))
          when (and (eq (outbox-message-status message) :pending)
                    (%outbox-available-p message now))
            do (push (%copy-outbox-message message) selected)
               (incf count))
    (nreverse selected)))

(defun %claim-outbox-messages (messages now limit lease-seconds)
  (let ((claimed nil)
        (count 0)
        (next nil))
    (dolist (message messages)
      (let ((claimable
              (or (and (eq (outbox-message-status message) :pending)
                       (%outbox-available-p message now))
                  (%outbox-expired-p message now lease-seconds))))
        (if (and claimable (or (null limit) (< count limit)))
            (let* ((attempts (1+ (outbox-message-attempts message)))
                   (token (list :claim now attempts))
                   (claimed-message
                     (%copy-outbox-message
                      message
                      :status :in-flight
                      :attempts attempts
                      :claimed-at now
                      :claim-token token)))
              (push claimed-message claimed)
              (push claimed-message next)
              (incf count))
              (push (%copy-outbox-message message) next))))
    (values (nreverse next) (nreverse claimed))))

(defun %claim-in-memory-outbox-messages
    (messages now limit lease-seconds)
  (labels ((claimable-p (message)
             (or (and (eq (outbox-message-status message) :pending)
                      (%outbox-available-p message now))
                 (%outbox-expired-p message now lease-seconds)))
           (claim-message (message)
             (let* ((attempts (1+ (outbox-message-attempts message)))
                    (token (list :claim now attempts))
                    (claimed-message
                      (%copy-outbox-message
                       message
                       :status :in-flight
                       :attempts attempts
                       :claimed-at now
                       :claim-token token)))
               (%replace-outbox-message-state message claimed-message)
               claimed-message)))
    (let ((claimed nil)
          (count 0))
      (loop for message across messages
            while (or (null limit) (< count limit))
            when (claimable-p message)
              do (push (claim-message message) claimed)
                 (incf count))
      (values messages (nreverse claimed)))))

(defmethod outbox-read-pending ((store in-memory-outbox-store)
                                &key (now *unspecified*) limit)
  (let ((now (%outbox-now now)))
    (%validate-outbox-time now)
    (%validate-outbox-limit limit)
    (%with-durable-lock ((%in-memory-outbox-lock store))
      (%pending-in-memory-outbox-messages
       (%in-memory-outbox-ordered-messages store) now limit))))

(defmethod outbox-read-pending ((store file-outbox-store)
                                &key (now *unspecified*) limit)
  (let ((now (%outbox-now now)))
    (%validate-outbox-time now)
    (%validate-outbox-limit limit)
    (%with-durable-lock ((%file-outbox-lock store))
      (let ((messages (%pending-outbox-messages
                       (%read-outbox-messages store) now)))
                (mapcar #'%copy-outbox-message
                        (if limit (subseq messages 0 (min limit (length messages)))
                  messages))))))

(defmethod outbox-read ((store in-memory-outbox-store) message-id)
  (%with-durable-lock ((%in-memory-outbox-lock store))
    (let ((message (gethash message-id (%in-memory-outbox-index store))))
      (and message (%copy-outbox-message message)))))

(defmethod outbox-read ((store file-outbox-store) message-id)
  (%with-durable-lock ((%file-outbox-lock store))
    (let ((message (%outbox-find (%read-outbox-messages store) message-id)))
      (and message (%copy-outbox-message message)))))

(defmethod outbox-read-all ((store in-memory-outbox-store)
                            &key (status *unspecified*) limit)
  (%validate-outbox-status status)
  (%validate-outbox-limit limit)
  (%with-durable-lock ((%in-memory-outbox-lock store))
    (%select-in-memory-outbox-messages
     (%in-memory-outbox-ordered-messages store) status limit)))

(defmethod outbox-read-all ((store file-outbox-store)
                            &key (status *unspecified*) limit)
  (%validate-outbox-status status)
  (%validate-outbox-limit limit)
  (%with-durable-lock ((%file-outbox-lock store))
    (%select-outbox-messages (%read-outbox-messages store)
                             status
                             limit)))

(defmethod outbox-claim ((store in-memory-outbox-store)
                         &key
                         (now *unspecified*)
                         limit
                         (lease-seconds *unspecified* lease-seconds-supplied-p))
  (unless lease-seconds-supplied-p
    (setf lease-seconds 60))
  (let ((now (%outbox-now now)))
    (%validate-outbox-time now)
    (%validate-outbox-limit limit)
    (%validate-outbox-lease lease-seconds)
    (%with-durable-lock ((%in-memory-outbox-lock store))
      (let ((current (%in-memory-outbox-ordered-messages store)))
        (nth-value 1
                   (%claim-in-memory-outbox-messages
                    current now limit lease-seconds))))))

(defmethod outbox-claim ((store file-outbox-store)
                         &key
                         (now *unspecified*)
                         limit
                         (lease-seconds *unspecified* lease-seconds-supplied-p))
  (unless lease-seconds-supplied-p
    (setf lease-seconds 60))
  (let ((now (%outbox-now now)))
    (%validate-outbox-time now)
    (%validate-outbox-limit limit)
    (%validate-outbox-lease lease-seconds)
    (%with-durable-lock ((%file-outbox-lock store))
      (let ((old (%read-outbox-messages store)))
        (multiple-value-bind (next claimed)
            (%claim-outbox-messages old now limit lease-seconds)
          (when claimed (%write-outbox-messages store next))
          claimed)))))
