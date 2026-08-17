(defun %invalid-domain-event (event reason &optional message)
  (error
   'invalid-domain-event
   :event
   event
   :reason
   reason
   :message
   (or message "The domain event is invalid.")))

(defun %signal-unsupported-event-store-operation (store operation)
  (error 'event-store-operation-not-supported :operation operation :store store))

(defun %truncate-condition-payload (value)
  "Bound a caller- or corruption-derived string before a condition retains
it, so an oversized rejected payload is not kept alive in full for the
condition's lifetime -- printing, logging, or serializing the condition
later would otherwise re-materialize it in full.

Conditions are not STANDARD-OBJECTs in SBCL's condition-class metaclass, so
MAKE-CONDITION does not dispatch through INITIALIZE-INSTANCE or
SHARED-INITIALIZE the way MAKE-INSTANCE would; a :AFTER method on either
never runs. Callers must therefore truncate the value they pass at each
construction site rather than relying on a class-level hook."
  (let ((max-length 2048))
    (if (and (stringp value) (> (length value) max-length))
        (format nil "~A...<truncated, ~D characters>"
                (subseq value 0 max-length)
                (length value))
        value)))
