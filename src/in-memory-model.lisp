(in-package #:cl-event-sourcing-kit)

(defclass in-memory-event-store ()
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
   (stream-versions
    :initform
    (make-hash-table :test #'equal)
    :accessor
    %in-memory-stream-versions)
   (snapshots
    :initform
    (make-hash-table :test #'equal)
    :accessor
    %in-memory-snapshots)
   (global-events :initform nil :accessor %in-memory-global-events)
   (global-ordered-events
    :initarg
    :global-ordered-events
    :initform
    (make-array 16 :adjustable t :fill-pointer 0)
    :accessor
    %in-memory-global-ordered-events)
   (global-position
    :initarg
    :global-position-start
    :initform
    0
    :accessor
    %in-memory-global-position)
   (global-position-floor
    :initarg
    :global-position-floor
    :initform
    0
    :accessor
    %in-memory-global-position-floor)
   (lock :initarg :lock :accessor %in-memory-lock)))

(defmacro %with-in-memory-lock ((store) &body body)
  `(cl-concurrent-kit:with-lock-held ((%in-memory-lock ,store)) ,@body))
