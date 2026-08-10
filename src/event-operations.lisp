(defmethod print-object ((event domain-event) stream)
  (print-unreadable-object
   (event stream :type t :identity nil)
   (format
    stream
    "~S ~S ~S v~S"
    (domain-event-id event)
    (domain-event-type event)
    (domain-event-stream-id event)
    (domain-event-version event))))

(defun %ensure-non-nil (value event reason)
  (unless value
    (%invalid-domain-event event reason)))

(defun %ensure-non-negative-integer-or-nil (value event reason)
  (unless (or (null value) (and (integerp value) (<= 0 value)))
    (%invalid-domain-event event reason)))

(defun %ensure-non-negative-integer (value event reason)
  (unless (and (integerp value) (<= 0 value))
    (%invalid-domain-event event reason)))

(defun %call-id-source (source event)
  (let ((value
         (handler-case (if (functionp source) (funcall source)
                         (cl-boundary-kit:uuid-generate source))
           (error ()
             (%invalid-domain-event event :id-source)))))
    (%ensure-non-nil value event :id)
    value))

(defun %call-clock (clock event)
  (let ((value
         (handler-case (if (functionp clock) (funcall clock)
                         (cl-boundary-kit:clock-now clock))
           (error ()
             (%invalid-domain-event event :clock)))))
    value))

(defun make-domain-event (&key
                          (id *unspecified*)
                          (type *unspecified*)
                          (stream-id *unspecified*)
                          (aggregate-id *unspecified*)
                          payload
                          metadata
                          (timestamp *unspecified*)
                          (schema-version *unspecified*)
                          version
                          correlation-id
                          causation-id
                          global-position
                          (id-source *unspecified*)
                          (clock *unspecified*))
  "Create an immutable, domain-independent event envelope.

TYPE and STREAM-ID are required and may be any non-NIL comparable values.
AGGREGATE-ID defaults to STREAM-ID but may be a distinct opaque identity.
Omitted ID and TIMESTAMP values are obtained from ID-SOURCE and CLOCK; both
accept either a function or the corresponding CL-BOUNDARY-KIT source object.
SCHEMA-VERSION identifies the serialized shape of the event payload and must
be a non-negative integer. PAYLOAD and METADATA are retained without
serialization or deep copying."
  (let ((event nil))
    (when (eq schema-version *unspecified*)
      (setf schema-version 1))
    (when (eq type *unspecified*)
      (%invalid-domain-event event :type))
    (%ensure-non-nil type event :type)
    (when (eq stream-id *unspecified*)
      (%invalid-domain-event event :stream-id))
    (%ensure-non-nil stream-id event :stream-id)
    (when (and (not (eq aggregate-id *unspecified*)) (null aggregate-id))
      (%invalid-domain-event event :aggregate-id))
    (%ensure-non-negative-integer schema-version event :schema-version)
    (%ensure-non-negative-integer-or-nil version event :version)
    (%ensure-non-negative-integer-or-nil global-position event :global-position)
    (let ((resolved-id
           (if (eq id *unspecified*) (%call-id-source
                                      (if (eq id-source *unspecified*) *default-event-id-source*
                                        id-source)
                                      event)
             id))
          (resolved-timestamp
           (if (eq timestamp *unspecified*) (%call-clock
                                             (if (eq clock *unspecified*) *default-event-clock*
                                               clock)
                                             event)
             timestamp))
          (resolved-aggregate-id
           (if (eq aggregate-id *unspecified*) stream-id
             aggregate-id)))
      (%ensure-non-nil resolved-id event :id)
      (%make-domain-event
       resolved-id
       type
       stream-id
       resolved-aggregate-id
       payload
       metadata
       resolved-timestamp
       schema-version
       version
       correlation-id
       causation-id
       global-position))))
