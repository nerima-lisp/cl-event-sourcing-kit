(in-package #:cl-event-sourcing-kit/test)

(defun make-test-event (id
                        stream-id
                        payload
                        &key
                        (type :test-event)
                        (timestamp 100)
                        metadata
                        correlation-id
                        causation-id
                        version
                        global-position
                        aggregate-id)
  (apply
   #'make-domain-event
   (append
    (list
     :id
     id
     :type
     type
     :stream-id
     stream-id
     :payload
     payload
     :metadata
     metadata
     :timestamp
     timestamp
     :correlation-id
     correlation-id
     :causation-id
     causation-id
     :version
     version
     :global-position
     global-position)
    (when aggregate-id
      (list :aggregate-id aggregate-id)))))

(defclass unsupported-store (event-store)
  ())

(defclass protocol-store (event-store)
  ())

(defmethod event-store-append ((store protocol-store)
                               stream-id
                               events
                               &key
                               (expected-version :any))
  (declare (ignore store stream-id expected-version))
  (values events (length events)))
