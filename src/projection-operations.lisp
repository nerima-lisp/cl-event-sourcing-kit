(defun projection-p (object)
  (typep object 'projection))

(defun %validate-projection-checkpoint (checkpoint)
  (unless (and (integerp checkpoint) (<= 0 checkpoint))
    (error 'type-error :datum checkpoint :expected-type '(integer 0 *)))
  checkpoint)

(defun make-projection (&key
                        name
                        initial-state
                        (initial-state-factory *unspecified*)
                        (state *unspecified*)
                        (handler *unspecified*)
                        (checkpoint *unspecified*))
  "Create a projection whose HANDLER folds one committed event.

INITIAL-STATE is retained as an opaque value.  Use INITIAL-STATE-FACTORY when
the state is mutable and each rebuild needs a fresh value.  HANDLER receives
the current state and event and returns the next state; checkpoint mutation
occurs only after that call succeeds."
  (unless (functionp handler)
    (error 'type-error :datum handler :expected-type 'function))
  (when (and
         (not (eq initial-state-factory *unspecified*))
         (not (functionp initial-state-factory)))
    (error 'type-error :datum initial-state-factory :expected-type 'function))
  (let* ((checkpoint
          (if (eq checkpoint *unspecified*) 0
            checkpoint))
         (factory
          (if (eq initial-state-factory *unspecified*) nil
            initial-state-factory)))
    (%validate-projection-checkpoint checkpoint)
    (make-instance
     'projection
     :name
     name
     :initial-state
     initial-state
     :initial-state-factory
     factory
     :state
     (if (eq state *unspecified*) (if factory (funcall factory)
                                    initial-state)
       state)
     :handler
     handler
     :checkpoint
     checkpoint)))

(defun %fresh-projection-state (projection)
  (let ((factory (%projection-initial-state-factory projection)))
    (if factory (funcall factory)
      (%projection-initial-state projection))))
