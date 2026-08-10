(in-package #:cl-event-sourcing-kit)

(defparameter *unspecified* (gensym "UNSPECIFIED-"))

(defparameter *default-event-id-source* (cl-boundary-kit:make-uuid-source))

(defparameter *default-event-clock* (cl-boundary-kit:make-clock))

(defstruct (domain-event
            (:constructor
             %make-domain-event
             (id
              type
              stream-id
              aggregate-id
              payload
              metadata
              timestamp
              schema-version
              version
              correlation-id
              causation-id
              global-position))
            (:copier nil)) (id nil :read-only t)
  (type nil :read-only t)
  (stream-id nil :read-only t)
  (aggregate-id nil :read-only t)
  (payload nil :read-only t)
  (metadata nil :read-only t)
  (timestamp nil :read-only t)
  (schema-version 1 :read-only t)
  (version nil :read-only t)
  (correlation-id nil :read-only t)
  (causation-id nil :read-only t)
  (global-position nil :read-only t))
