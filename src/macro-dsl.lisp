(in-package #:cl-event-sourcing-kit)

(defmacro define-event-reducer (name (state-var event-var) &body clauses)
  "Define a type-dispatching aggregate reducer.

Each clause is `(EVENT-TYPE FORMS...)`; `:otherwise` is optional and becomes
the default branch.  The macro only generates dispatch logic; state remains
caller-owned and event payloads remain opaque."
  (let ((otherwise-body nil)
        (cases nil))
    (dolist (clause clauses)
      (destructuring-bind (key &body body) clause
        (if (eq key :otherwise) (setf otherwise-body body)
          (push
           `(,key
             (progn
               ,@body))
           cases))))
    `(defun ,name (,state-var ,event-var)
       (cond
         ,@(mapcar (lambda (case)
                     `((or
                       ,@(mapcar
                          (lambda (key)
                            `(equal (domain-event-type ,event-var) ',key))
                          (if (listp (first case))
                              (first case)
                            (list (first case)))))
                       ,(second case)))
                   (nreverse cases))
         (t
          ,(if otherwise-body `(progn
                                 ,@otherwise-body)
             `(error
               'event-sourcing-error
               :message
               "No reducer clause matched the event type.")))))))

(defmacro with-event-staging ((variable
                               stream-id
                               &key
                               (expected-version :no-stream))
                              &body
                              body)
  "Lexically bind a staging session for STREAM-ID."
  `(let ((,variable
          (make-event-staging ,stream-id :expected-version ,expected-version)))
     ,@body))
