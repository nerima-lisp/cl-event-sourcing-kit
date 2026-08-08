(in-package #:cl-event-sourcing-kit)

(defclass in-memory-event-store (event-store)
  ((streams
    :initform
    (make-hash-table :test #'equal)
    :accessor
    %in-memory-streams)
   (event-index
    :initform
    (make-hash-table :test #'equal)
    :accessor
    %in-memory-event-index)
   (global-events :initform nil :accessor %in-memory-global-events)
   (global-position
    :initarg
    :global-position-start
    :initform
    0
    :accessor
    %in-memory-global-position)
   (lock :initarg :lock :accessor %in-memory-lock)))

(defmacro %with-in-memory-lock ((store) &body body)
  `(cl-concurrent-kit:with-lock-held ((%in-memory-lock ,store)) ,@body))
