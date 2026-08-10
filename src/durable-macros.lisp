(in-package #:cl-event-sourcing-kit)

(defparameter *durable-lock-registry* (make-hash-table :test #'equal))

(defparameter *durable-lock-registry-lock*
  (cl-concurrent-kit:make-lock
   :name "cl-event-sourcing-kit/durable-lock-registry"))

(defmacro %with-durable-lock ((lock) &body body)
  `(cl-concurrent-kit:with-lock-held (,lock) ,@body))

(defmacro with-retries ((&key
                          (attempts 3)
                          (delay 0)
                          (backoff 1)
                          retry-if)
                        &body body)
  (let ((attempts-var (gensym "ATTEMPTS-"))
        (delay-var (gensym "DELAY-"))
        (backoff-var (gensym "BACKOFF-"))
        (predicate-var (gensym "RETRY-IF-"))
        (current-delay-var (gensym "CURRENT-DELAY-")))
    `(let* ((,attempts-var ,attempts)
            (,delay-var ,delay)
            (,backoff-var ,backoff)
            (,predicate-var ,retry-if)
            (,current-delay-var ,delay))
       (unless (and (integerp ,attempts-var) (plusp ,attempts-var))
         (error 'event-sourcing-error
                :message "WITH-RETRIES requires a positive attempt count."))
       (unless (and (realp ,delay-var) (not (minusp ,delay-var)))
         (error 'event-sourcing-error
                :message "WITH-RETRIES requires a non-negative delay."))
       (unless (and (realp ,backoff-var) (plusp ,backoff-var))
         (error 'event-sourcing-error
                :message "WITH-RETRIES requires a positive backoff."))
       (when (and ,predicate-var (not (functionp ,predicate-var)))
         (error 'type-error :datum ,predicate-var :expected-type 'function))
       (loop for attempt from 1 to ,attempts-var
             do (handler-case
                   (return (progn ,@body))
                 (error (cause)
                   (if (or (= attempt ,attempts-var)
                           (and ,predicate-var
                                (not (funcall ,predicate-var cause))))
                       (error cause)
                       (progn
                         (when (plusp ,current-delay-var)
                           (sleep ,current-delay-var))
                         (setf ,current-delay-var
                               (* ,current-delay-var ,backoff-var))))))))))
