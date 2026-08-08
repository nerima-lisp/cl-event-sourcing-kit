(defun replay-events (initial-state events reducer)
  "Fold EVENTS from left to right with REDUCER.

The reducer receives STATE and one immutable DOMAIN-EVENT and returns the next
state.  No sorting, persistence, serialization, or mutation of EVENTS is
performed."
  (unless (functionp reducer)
    (error 'type-error :datum reducer :expected-type 'function))
  (let ((state initial-state))
    (dolist (event (%coerce-event-list events) state)
      (unless (domain-event-p event)
        (%invalid-domain-event event :event-type))
      (setf state (funcall reducer state event)))))
