(in-package #:cl-event-sourcing-kit)

(defparameter *durable-lock-registry* (make-hash-table :test #'equal))

(defparameter *durable-lock-registry-lock*
  (cl-concurrent-kit:make-lock
   :name "cl-event-sourcing-kit/durable-lock-registry"))

(defmacro %with-durable-lock ((lock) &body body)
  `(cl-concurrent-kit:with-lock-held (,lock) ,@body))
