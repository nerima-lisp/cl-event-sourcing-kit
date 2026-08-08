(in-package #:cl-event-sourcing-kit)

(defmacro define-projection (name
                             &key
                             (initial-state nil)
                             (initial-state-factory nil factory-p)
                             (handler nil handler-p))
  "Define a zero-argument constructor for a fresh PROJECTION.

HANDLER is an expression that evaluates to a function accepting STATE and
EVENT.  Supply INITIAL-STATE-FACTORY when the initial value is mutable; the
factory is called for every rebuild."
  (unless handler-p
    (error "DEFINE-PROJECTION ~S requires :HANDLER." name))
  `(defun ,name ()
     (make-projection
      :name
      ',name
      :initial-state
      ,initial-state
      ,@(when factory-p
          `(:initial-state-factory ,initial-state-factory))
      :handler
      ,handler)))
