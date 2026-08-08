(in-package #:cl-event-sourcing-kit)

(defclass event-store ()
  ()
  (:documentation
   "Abstract event-store protocol.

An adapter subclasses this class and implements the generic operations.  The
class is intentionally not an adapter wrapper or factory: storage ownership,
serialization, transactions, durability, and recovery remain with the
adapter."))

(defgeneric event-store-append (store stream-id events &key expected-version))

(defgeneric event-store-read (store stream-id &key from-version to-version))

(defgeneric event-store-read-all (store &key after-global-position limit))

(defgeneric event-store-current-version (store stream-id))

(defgeneric event-store-current-global-position (store))

(defgeneric event-store-stream-exists-p (store stream-id))

(defgeneric event-store-global-position-supported-p (store))

(defgeneric event-store-event-equivalent-p (store
                                            existing-event
                                            requested-event))
