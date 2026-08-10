(defun event-serializer-p (object)
  (typep object 'event-serializer))

(defun make-event-serializer (&key
                                      encode
                                      decode
                                      (max-bytes *unspecified* max-bytes-supplied-p)
                                      (max-depth *unspecified* max-depth-supplied-p))
  "Create a serializer using ENCODE and DECODE functions.

When either function is NIL, the durable reference implementation uses its
safe, portable S-expression codec.  Applications with UUID, timestamp, or
binary payload types should provide an application codec explicitly."
  (unless max-bytes-supplied-p
    (setf max-bytes 1048576))
  (unless max-depth-supplied-p
    (setf max-depth 64))
  (when (and encode (not (functionp encode)))
    (error 'type-error :datum encode :expected-type 'function))
  (when (and decode (not (functionp decode)))
    (error 'type-error :datum decode :expected-type 'function))
  (unless (and (integerp max-bytes) (plusp max-bytes))
    (error 'type-error :datum max-bytes :expected-type '(integer 1 *)))
  (unless (and (integerp max-depth) (plusp max-depth))
    (error 'type-error :datum max-depth :expected-type '(integer 1 *)))
  (make-instance 'event-serializer
                 :encode encode
                 :decode decode
                 :max-bytes max-bytes
                 :max-depth max-depth))

(defun file-event-store-p (object)
  (typep object 'file-event-store))

(defun subscription-offset-store-p (object)
  (typep object 'subscription-offset-store))

(defun in-memory-subscription-offset-store-p (object)
  (typep object 'in-memory-subscription-offset-store))

(defun file-subscription-offset-store-p (object)
  (typep object 'file-subscription-offset-store))

(defun subscription-lease-store-p (object)
  (typep object 'subscription-lease-store))

(defun in-memory-subscription-lease-store-p (object)
  (typep object 'in-memory-subscription-lease-store))

(defun subscription-p (object)
  (typep object 'subscription))

(defun make-outbox-message (&key
                            (id *unspecified*)
                            (topic *unspecified*)
                            payload
                            metadata
                            (created-at (get-universal-time))
                            (status *unspecified* status-supplied-p)
                            (attempts *unspecified* attempts-supplied-p)
                            claimed-at
                            (available-at created-at)
                            claim-token
                            last-error
                            dead-lettered-at)
  "Create an application-level message for an outbox.

The ID is the idempotency key used by an outbox adapter.  Payload and
metadata remain opaque and are serialized by the selected adapter."
  (unless status-supplied-p
    (setf status :pending))
  (unless attempts-supplied-p
    (setf attempts 0))
  (when (or (eq id *unspecified*) (null id))
    (error 'event-sourcing-error
           :message "An outbox message requires a non-NIL id."))
  (when (or (eq topic *unspecified*) (null topic))
    (error 'event-sourcing-error
           :message "An outbox message requires a non-NIL topic."))
  (unless (and (integerp attempts) (<= 0 attempts))
    (error 'event-sourcing-error
           :message "An outbox message attempt count must be non-negative."))
  (unless (member status '(:pending :in-flight :delivered :dead-letter) :test #'eq)
    (error 'event-sourcing-error
           :message "An outbox message has an invalid status."))
  (unless (or (null last-error) (stringp last-error))
    (error 'type-error :datum last-error :expected-type '(or null string)))
  (unless (or (null dead-lettered-at) (realp dead-lettered-at))
    (error 'type-error :datum dead-lettered-at :expected-type '(or null real)))
  (%make-outbox-message id topic payload metadata created-at status attempts
                        claimed-at available-at claim-token last-error
                        dead-lettered-at))

(defun outbox-store-p (object)
  (typep object 'outbox-store))

(defun in-memory-outbox-store-p (object)
  (typep object 'in-memory-outbox-store))

(defun file-outbox-store-p (object)
  (typep object 'file-outbox-store))

(defun event-outbox-store-p (object)
  (typep object 'event-outbox-store))

(defun make-projection-checkpoint-record (&key
                                         state
                                         (position *unspecified*)
                                         updated-at)
  (unless (and (integerp position) (<= 0 position))
    (error 'event-sourcing-error
           :message "A projection checkpoint position must be non-negative."))
  (%make-projection-checkpoint-record state position updated-at))

(defun projection-checkpoint-store-p (object)
  (typep object 'projection-checkpoint-store))

(defun in-memory-projection-checkpoint-store-p (object)
  (typep object 'in-memory-projection-checkpoint-store))

(defun file-projection-checkpoint-store-p (object)
  (typep object 'file-projection-checkpoint-store))

(defun durable-projection-runner-p (object)
  (typep object 'durable-projection-runner))

(defun upcaster-registry-p (object)
  (typep object 'upcaster-registry))

(defun observed-event-store-p (object)
  (typep object 'observed-event-store))

(defun make-observed-event-store (store &key before after on-error)
  "Wrap STORE with lifecycle callbacks.

BEFORE receives the operation keyword before execution, AFTER receives it
after success, and ON-ERROR receives the operation keyword and condition."
  (unless (typep store 'event-store)
    (error 'type-error :datum store :expected-type 'event-store))
  (dolist (callback (list before after on-error))
    (when (and callback (not (functionp callback)))
      (error 'type-error :datum callback :expected-type 'function)))
  (make-instance 'observed-event-store
                 :delegate store
                 :before before
                 :after after
                 :on-error on-error))
