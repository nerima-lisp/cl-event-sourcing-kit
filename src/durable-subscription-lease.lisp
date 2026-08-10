;;;; Consumer leases and fencing tokens

(defun %validate-lease-identity (value description)
  (when (or (eq value *unspecified*) (null value))
    (error 'event-sourcing-error :message description))
  value)

(defun %validate-lease-now (now)
  (unless (realp now)
    (error 'type-error :datum now :expected-type 'real))
  now)

(defun %validate-lease-seconds (lease-seconds)
  (unless (and (realp lease-seconds) (plusp lease-seconds))
    (error 'type-error :datum lease-seconds :expected-type '(real 0 *)))
  lease-seconds)

(defun %validate-subscription-lease (lease)
  (unless (subscription-lease-p lease)
    (error 'type-error :datum lease :expected-type 'subscription-lease))
  lease)

(defun %lease-expired-p (lease now)
  (<= (subscription-lease-expires-at lease) now))

(defun make-subscription-lease (&key
                                (consumer-id *unspecified*)
                                (owner-id *unspecified*)
                                (fencing-token *unspecified*)
                                (expires-at *unspecified*))
  "Construct a lease value for a custom lease-store adapter.

Adapters normally return values created by this function from their
transactional acquire/renew operations.  FENCING-TOKEN must increase for a
consumer whenever ownership changes; consumers should include it in every
side effect that needs stale-owner protection."
  (%validate-lease-identity
   consumer-id
   "A subscription lease requires a consumer id.")
  (%validate-lease-identity
   owner-id
   "A subscription lease requires an owner id.")
  (unless (and (integerp fencing-token) (plusp fencing-token))
    (error 'type-error
           :datum fencing-token
           :expected-type '(integer 1 *)))
  (unless (realp expires-at)
    (error 'type-error :datum expires-at :expected-type 'real))
  (%make-subscription-lease consumer-id owner-id fencing-token expires-at))

(defmethod subscription-lease-store-capabilities
    ((store subscription-lease-store))
  (declare (ignore store))
  nil)

(defmethod subscription-lease-acquire
    ((store subscription-lease-store) consumer-id owner-id
     &key now lease-seconds)
  (declare (ignore consumer-id owner-id now lease-seconds))
  (error 'event-store-operation-not-supported
         :operation :subscription-lease-acquire
         :store store))

(defmethod subscription-lease-renew
    ((store subscription-lease-store) lease &key now lease-seconds)
  (declare (ignore lease now lease-seconds))
  (error 'event-store-operation-not-supported
         :operation :subscription-lease-renew
         :store store))

(defmethod subscription-lease-release
    ((store subscription-lease-store) lease)
  (declare (ignore lease))
  (error 'event-store-operation-not-supported
         :operation :subscription-lease-release
         :store store))

(defmethod subscription-lease-valid-p
    ((store subscription-lease-store) lease &key now)
  (declare (ignore store now))
  (%validate-subscription-lease lease)
  nil)

(defun make-in-memory-subscription-lease-store (&key lock)
  "Create the process-local reference lease store.

The implementation is safe for threads in one Lisp image.  A multi-process
or multi-host deployment must implement the same generic protocol with a
transactional lease and fencing-token service, then advertise the stronger
deployment guarantees at its event-store boundary."
  (make-instance 'in-memory-subscription-lease-store
                 :leases (make-hash-table :test #'equal)
                 :next-tokens (make-hash-table :test #'equal)
                 :lock (or lock (%durable-lock "subscription-leases"))))

(defmethod subscription-lease-store-capabilities
    ((store in-memory-subscription-lease-store))
  (declare (ignore store))
  '(:consumer-leases :fencing :process-local-lock))

(defmethod subscription-lease-acquire
    ((store in-memory-subscription-lease-store) consumer-id owner-id
     &key
     (now (get-universal-time))
     (lease-seconds *unspecified* lease-seconds-supplied-p))
  (unless lease-seconds-supplied-p
    (setf lease-seconds 60))
  (%validate-lease-identity
   consumer-id
   "Acquiring a subscription lease requires a consumer id.")
  (%validate-lease-identity
   owner-id
   "Acquiring a subscription lease requires an owner id.")
  (%validate-lease-now now)
  (%validate-lease-seconds lease-seconds)
  (%with-durable-lock ((%in-memory-subscription-lease-lock store))
    (let ((current (gethash consumer-id
                            (%in-memory-subscription-leases store))))
      (cond
        ((and current (not (%lease-expired-p current now)))
         (when (equal owner-id (subscription-lease-owner-id current))
           (let ((renewed
                   (make-subscription-lease
                    :consumer-id consumer-id
                    :owner-id owner-id
                    :fencing-token
                    (subscription-lease-fencing-token current)
                    :expires-at (+ now lease-seconds))))
             (setf (gethash consumer-id
                            (%in-memory-subscription-leases store))
                   renewed)
             renewed)))
        (t
         (let* ((next-token
                  (1+ (gethash consumer-id
                              (%in-memory-subscription-lease-next-tokens store)
                              0)))
                (lease
                  (make-subscription-lease
                   :consumer-id consumer-id
                   :owner-id owner-id
                   :fencing-token next-token
                   :expires-at (+ now lease-seconds))))
           (setf (gethash consumer-id
                          (%in-memory-subscription-lease-next-tokens store))
                 next-token
                 (gethash consumer-id
                          (%in-memory-subscription-leases store))
                 lease)
           lease))))))

(defmethod subscription-lease-renew
    ((store in-memory-subscription-lease-store) lease
     &key
     (now (get-universal-time))
     (lease-seconds *unspecified* lease-seconds-supplied-p))
  (unless lease-seconds-supplied-p
    (setf lease-seconds 60))
  (%validate-subscription-lease lease)
  (%validate-lease-now now)
  (%validate-lease-seconds lease-seconds)
  (%with-durable-lock ((%in-memory-subscription-lease-lock store))
    (let ((current
            (gethash (subscription-lease-consumer-id lease)
                     (%in-memory-subscription-leases store))))
      (when (and current
                 (equal (subscription-lease-owner-id current)
                        (subscription-lease-owner-id lease))
                 (= (subscription-lease-fencing-token current)
                    (subscription-lease-fencing-token lease))
                 (not (%lease-expired-p current now)))
        (let ((renewed
                (make-subscription-lease
                 :consumer-id (subscription-lease-consumer-id lease)
                 :owner-id (subscription-lease-owner-id lease)
                 :fencing-token (subscription-lease-fencing-token lease)
                 :expires-at (+ now lease-seconds))))
          (setf (gethash (subscription-lease-consumer-id lease)
                         (%in-memory-subscription-leases store))
                renewed)
          renewed)))))

(defmethod subscription-lease-release
    ((store in-memory-subscription-lease-store) lease)
  (%validate-subscription-lease lease)
  (%with-durable-lock ((%in-memory-subscription-lease-lock store))
    (let* ((consumer-id (subscription-lease-consumer-id lease))
           (current (gethash consumer-id
                             (%in-memory-subscription-leases store))))
      (when (and current
                 (equal (subscription-lease-owner-id current)
                        (subscription-lease-owner-id lease))
                 (= (subscription-lease-fencing-token current)
                    (subscription-lease-fencing-token lease)))
        (remhash consumer-id (%in-memory-subscription-leases store))
        t))))

(defmethod subscription-lease-valid-p
    ((store in-memory-subscription-lease-store) lease
     &key
     (now (get-universal-time)))
  (%validate-subscription-lease lease)
  (%validate-lease-now now)
  (%with-durable-lock ((%in-memory-subscription-lease-lock store))
    (let ((current
            (gethash (subscription-lease-consumer-id lease)
                     (%in-memory-subscription-leases store))))
      (and current
           (equal (subscription-lease-owner-id current)
                  (subscription-lease-owner-id lease))
           (= (subscription-lease-fencing-token current)
              (subscription-lease-fencing-token lease))
           (not (%lease-expired-p current now))))))

(defun %ensure-subscription-lease-locked
    (subscription &key (now (get-universal-time)))
  "Ensure SUBSCRIPTION owns a current fencing token.

The caller holds the subscription lock.  The lease-store operation is itself
atomic, so a competing process cannot pass this check with the same consumer
identity and a different owner."
  (let ((store (%subscription-lease-store subscription)))
    (if (null store)
        t
      (let* ((current (%subscription-lease subscription))
             (renewed
               (and current
                    (subscription-lease-renew
                     store
                     current
                     :now now
                     :lease-seconds
                     (subscription-lease-seconds subscription)))))
        (unless renewed
          (setf renewed
                (subscription-lease-acquire
                 store
                 (subscription-consumer-id subscription)
                 (subscription-owner-id subscription)
                 :now now
                 :lease-seconds (subscription-lease-seconds subscription))))
        (unless renewed
          (error 'event-sourcing-error
                 :message
                 "The subscription lease is unavailable or fenced by another owner."))
        (setf (%subscription-lease subscription) renewed)
        t))))

(defun subscription-current-lease (subscription)
  (unless (subscription-p subscription)
    (error 'type-error :datum subscription :expected-type 'subscription))
  (%subscription-lease subscription))

(defun subscription-renew-lease
    (subscription &key (now (get-universal-time)))
  "Acquire or renew SUBSCRIPTION's lease and return its fencing value.

Return NIL for a subscription that was created without a lease store.  Signal
an event-sourcing error when a configured consumer cannot own its lease."
  (unless (subscription-p subscription)
    (error 'type-error :datum subscription :expected-type 'subscription))
  (%validate-lease-now now)
  (%with-durable-lock ((%subscription-lock subscription))
    (if (%subscription-lease-store subscription)
        (progn
          (%ensure-subscription-lease-locked subscription :now now)
          (%subscription-lease subscription))
      nil)))

(defun subscription-release-lease (subscription)
  "Release SUBSCRIPTION's current lease, returning the store result.

The local lease is cleared even when the store says that its fencing token is
already stale; a caller must acquire a fresh token before continuing."
  (unless (subscription-p subscription)
    (error 'type-error :datum subscription :expected-type 'subscription))
  (%with-durable-lock ((%subscription-lock subscription))
    (let ((store (%subscription-lease-store subscription))
          (lease (%subscription-lease subscription)))
      (if (and store lease)
          (prog1 (subscription-lease-release store lease)
            (setf (%subscription-lease subscription) nil))
          nil))))
