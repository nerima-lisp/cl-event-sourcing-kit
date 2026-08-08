(in-package #:cl-event-sourcing-kit)

(defclass event-staging ()
  ((stream-id :initarg :stream-id :reader event-staging-stream-id)
   (expected-version
    :initarg
    :expected-version
    :reader
    event-staging-expected-version)
   (events :initform nil :accessor %event-staging-events)))
