(in-package #:cl-event-sourcing-kit)

(defclass projection ()
  ((name :initarg :name :initform nil :reader projection-name)
   (initial-state :initarg :initial-state :reader %projection-initial-state)
   (initial-state-factory
    :initarg
    :initial-state-factory
    :initform
    nil
    :reader
    %projection-initial-state-factory)
   (state :initarg :state :reader projection-state)
   (handler :initarg :handler :reader projection-handler)
   (checkpoint :initarg :checkpoint :reader projection-checkpoint)))
