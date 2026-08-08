(defun event-staging-p (object)
  (typep object 'event-staging))

(defun make-event-staging (stream-id &key (expected-version *unspecified*))
  "Create a mutable staging session for one stream.

The default :NO-STREAM expectation makes a new aggregate safe by default.
After a successful commit the session's expectation advances to the returned
stream version and its uncommitted list is cleared."
  (let ((expected-version
         (if (eq expected-version *unspecified*) :no-stream
           expected-version)))
    (unless stream-id
      (%invalid-domain-event nil :stream-id "A staging stream id is required."))
    (%validate-expected-version expected-version)
    (make-instance
     'event-staging
     :stream-id
     stream-id
     :expected-version
     expected-version)))

(defun %ensure-staged-event (staging event)
  (unless (domain-event-p event)
    (%invalid-domain-event event :event-type))
  (unless (equal
           (event-staging-stream-id staging)
           (domain-event-stream-id event))
    (%invalid-domain-event
     event
     :stream-id
     "The staged event belongs to a different stream."))
  (when (or
         (not (null (domain-event-version event)))
         (not (null (domain-event-global-position event))))
    (%invalid-domain-event
     event
     :assigned-position
     "Only uncommitted events may be staged."))
  event)

(defun stage-event (staging event)
  "Stage EVENT in input order and return STAGING."
  (check-type staging event-staging)
  (%ensure-staged-event staging event)
  (setf (%event-staging-events staging) (nconc
                                         (%event-staging-events staging)
                                         (list event)))
  staging)

(defun uncommitted-events (staging)
  (check-type staging event-staging)
  (copy-list (%event-staging-events staging)))

(defun commit-events (staging store &key (expected-version *unspecified*))
  "Append staged events and mutate STAGING only after a successful append."
  (check-type staging event-staging)
  (let ((resolved-version
         (if (eq expected-version *unspecified*) (event-staging-expected-version
                                                  staging)
           expected-version)))
    (multiple-value-bind (committed-events version) (event-store-append
                                                     store
                                                     (event-staging-stream-id
                                                      staging)
                                                     (uncommitted-events
                                                      staging)
                                                     :expected-version
                                                     resolved-version)
      (setf (%event-staging-events staging) nil
            (slot-value staging 'expected-version) version)
      (values committed-events version))))
