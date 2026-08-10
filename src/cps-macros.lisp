(in-package #:cl-event-sourcing-kit)

(defmacro define-cps-operation (name lambda-list &body body)
  "Define a synchronous operation with the standard CPS boundary.

LAMBDA-LIST must bind ON-SUCCESS and ON-ERROR.  Keeping the operation body
inside this macro makes every continuation entry point share the same
validation and condition-routing semantics while preserving its public
lambda list."
  `(defun ,name ,lambda-list
     (%call-continuation
      (lambda () ,@body)
      on-success
      on-error)))
