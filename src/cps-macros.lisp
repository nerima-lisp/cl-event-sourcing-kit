(in-package #:cl-event-sourcing-kit)

(defmacro define-cps-operation (name lambda-list &body body)
  "Define a synchronous operation with the standard CPS boundary.

LAMBDA-LIST must bind ON-SUCCESS and ON-ERROR.  Keeping the operation body
inside this macro makes every continuation entry point share the same
validation and condition-routing semantics while preserving its public
lambda list."
  (labels ((contains-symbol-p (form symbol)
             (if (consp form)
                 (or (contains-symbol-p (car form) symbol)
                     (contains-symbol-p (cdr form) symbol))
                 (and (symbolp form)
                      (string= (symbol-name form) (symbol-name symbol))))))
    (unless (contains-symbol-p lambda-list 'on-success)
      (error "~S must bind ON-SUCCESS in its lambda list: ~S"
             name lambda-list))
    (unless (contains-symbol-p lambda-list 'on-error)
      (error "~S must bind ON-ERROR in its lambda list: ~S"
             name lambda-list))
    (let ((continuation-name (gensym "CONTINUATION-")))
      `(defun ,name ,lambda-list
         (unless (functionp on-success)
           (error 'type-error :datum on-success :expected-type 'function))
         (unless (functionp on-error)
           (error 'type-error :datum on-error :expected-type 'function))
       (cl-weave:with-continuation-values
           (continuation-values ,continuation-name continuation-called)
         (%call-continuation
            (lambda () ,@body)
           (lambda (&rest values)
              (apply #',continuation-name (cons :success values)))
           (lambda (condition)
              (funcall #',continuation-name :error condition)))
         (case (first continuation-values)
           (:success (apply on-success (rest continuation-values)))
           (:error (funcall on-error (second continuation-values)))
           (otherwise
            (error "CPS operation ~S returned an invalid terminal status: ~S"
                   ',name
                   (first continuation-values)))))))))
