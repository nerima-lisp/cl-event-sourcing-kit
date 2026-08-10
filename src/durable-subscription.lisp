;;;; Durable subscription offsets and at-least-once global-feed delivery

(defun %validate-durable-key (key description)
  (when (or (eq key *unspecified*) (null key))
    (error 'event-sourcing-error :message description))
  key)

(defun %validate-durable-position (position)
  (%validate-read-bound position)
  position)

(defun make-in-memory-subscription-offset-store (&key lock)
  (make-instance 'in-memory-subscription-offset-store
                 :offsets (make-hash-table :test #'equal)
                 :lock (or lock (%durable-lock "subscription-offsets"))))

(defun make-file-subscription-offset-store (&key
                                             (path *unspecified*)
                                             serializer
                                             lock
                                             (sync #'finish-output))
  (%validate-durable-key
   path
   "A file subscription offset store requires a path.")
  (unless (functionp sync)
    (error 'type-error :datum sync :expected-type 'function))
  (make-instance 'file-subscription-offset-store
                 :path (%durable-pathname path)
                 :serializer (%resolve-event-serializer serializer)
                 :sync sync
                 :lock (or lock (%durable-lock "file-subscription-offsets"))))

(defun %subscription-offset-wire (consumer-id global-position)
  (list :subscription-offset
        :consumer-id consumer-id
        :global-position global-position))

(defun %subscription-offsets-wire (offsets)
  (let ((records nil))
    (maphash (lambda (consumer-id global-position)
               (push (%subscription-offset-wire consumer-id global-position)
                     records))
             offsets)
    (cons :subscription-offsets (nreverse records))))

(defun %subscription-offset-table-from-wire (wire path)
  (let ((offsets (make-hash-table :test #'equal)))
    (cond
      ((null wire) offsets)
      ((not (and (%proper-list-p wire)
                 (eq (first wire) :subscription-offsets)))
       (error 'durable-store-corruption
              :path path
              :record wire
              :cause "The offset file has an invalid envelope."))
      (t
       (dolist (record (rest wire))
         (unless (and (%proper-list-p record)
                      (eq (first record) :subscription-offset))
           (error 'durable-store-corruption
                  :path path
                  :record record
                  :cause "The offset file has an invalid record."))
         (let ((consumer-id (%required-wire-value record :consumer-id))
               (position (%required-wire-value record :global-position)))
           (%validate-durable-key
            consumer-id
            "A persisted subscription offset requires a consumer id.")
           (%validate-durable-position position)
           (when (nth-value 1 (gethash consumer-id offsets))
             (error 'durable-store-corruption
                    :path path
                    :record record
                    :cause "The offset file contains a duplicate consumer id."))
           (setf (gethash consumer-id offsets) position)))))
    offsets))

(defun %read-subscription-offsets (store)
  (handler-case
      (%subscription-offset-table-from-wire
       (%read-serialized-file (%file-offset-path store)
                              (%file-offset-serializer store))
       (%file-offset-path store))
    (durable-store-corruption (condition) (error condition))
    (error (cause)
      (error 'durable-store-corruption
             :path (%file-offset-path store)
             :cause cause))))

(defun %write-subscription-offsets (store offsets)
  (%write-serialized-file (%file-offset-path store)
                          (%subscription-offsets-wire offsets)
                          (%file-offset-serializer store)
                          (%file-offset-sync store)))

(defmethod subscription-offset ((store in-memory-subscription-offset-store)
                                consumer-id)
  (%validate-durable-key
   consumer-id
   "A subscription offset requires a consumer id.")
  (%with-durable-lock ((%in-memory-offset-lock store))
    (gethash consumer-id (%in-memory-offsets store) 0)))

(defmethod save-subscription-offset
    ((store in-memory-subscription-offset-store)
     consumer-id
     global-position)
  (%validate-durable-key
   consumer-id
   "A subscription offset requires a consumer id.")
  (%validate-durable-position global-position)
  (%with-durable-lock ((%in-memory-offset-lock store))
    (let ((old (gethash consumer-id (%in-memory-offsets store) 0)))
      (when (< global-position old)
        (error 'event-sourcing-error
               :message "A subscription offset cannot move backwards."))
      (setf (gethash consumer-id (%in-memory-offsets store)) global-position)
      global-position)))

(defmethod subscription-offset ((store file-subscription-offset-store)
                                consumer-id)
  (%validate-durable-key
   consumer-id
   "A subscription offset requires a consumer id.")
  (%with-durable-lock ((%file-offset-lock store))
    (gethash consumer-id (%read-subscription-offsets store) 0)))

(defmethod save-subscription-offset
    ((store file-subscription-offset-store)
     consumer-id
     global-position)
  (%validate-durable-key
   consumer-id
   "A subscription offset requires a consumer id.")
  (%validate-durable-position global-position)
  (%with-durable-lock ((%file-offset-lock store))
    (let ((offsets (%read-subscription-offsets store))
          (old 0))
      (setf old (gethash consumer-id offsets 0))
      (when (< global-position old)
        (error 'event-sourcing-error
               :message "A subscription offset cannot move backwards."))
      (setf (gethash consumer-id offsets) global-position)
      (%write-subscription-offsets store offsets)
      global-position)))

(defun make-subscription (&key
                           (event-store *unspecified*)
                           (offset-store *unspecified*)
                           (consumer-id *unspecified*)
                           (cursor *unspecified*)
                           (batch-size *unspecified* batch-size-supplied-p)
                           filter
                           lock
                           lease-store
                           owner-id
                           (lease-seconds *unspecified* lease-seconds-supplied-p))
  (unless batch-size-supplied-p
    (setf batch-size 100))
  (unless lease-seconds-supplied-p
    (setf lease-seconds 60))
  (unless (typep event-store 'event-store)
    (error 'type-error :datum event-store :expected-type 'event-store))
  (unless (subscription-offset-store-p offset-store)
    (error 'type-error
           :datum offset-store
           :expected-type 'subscription-offset-store))
  (%validate-durable-key
   consumer-id
   "A subscription requires a non-NIL consumer id.")
  (unless (and (integerp batch-size) (plusp batch-size))
    (error 'type-error :datum batch-size :expected-type '(integer 1 *)))
  (when (and filter (not (functionp filter)))
    (error 'type-error :datum filter :expected-type 'function))
  (when lease-store
    (unless (subscription-lease-store-p lease-store)
      (error 'type-error
             :datum lease-store
             :expected-type 'subscription-lease-store))
    (%validate-durable-key
     owner-id
     "A leased subscription requires an owner id.")
    (%validate-lease-seconds lease-seconds))
  (when (and (null lease-store) owner-id)
    (error 'event-sourcing-error
           :message
           "An owner id requires a subscription lease store."))
  (unless (event-store-global-position-supported-p event-store)
    (error 'event-store-operation-not-supported
           :operation 'make-subscription
           :store event-store))
  (let ((saved (subscription-offset offset-store consumer-id)))
    (when (and (not (eq cursor *unspecified*))
               (< (%validate-durable-position cursor) saved))
      (error 'event-sourcing-error
             :message "A subscription cursor cannot precede its durable offset."))
    (make-instance 'subscription
                   :event-store event-store
                   :offset-store offset-store
                   :consumer-id consumer-id
                   :cursor (if (eq cursor *unspecified*) saved cursor)
                   :batch-size batch-size
                   :filter filter
                   :lock (or lock (%durable-lock "subscription"))
                   :in-flight nil
                   :lease-store lease-store
                   :owner-id owner-id
                   :lease-seconds lease-seconds
                   :lease nil)))

(defmethod subscription-poll ((subscription subscription))
  (%with-durable-lock ((%subscription-lock subscription))
    (%ensure-subscription-lease-locked subscription)
    (or (%subscription-in-flight subscription)
        (let ((events
                (event-store-read-all
                 (%subscription-event-store subscription)
                 :after-global-position (subscription-cursor subscription)
                 :limit (%subscription-batch-size subscription))))
          (when events
            (let* ((after (domain-event-global-position (car (last events))))
                   (selected
                     (if (%subscription-filter subscription)
                         (remove-if-not (%subscription-filter subscription) events)
                       events)))
              (if selected
                  (setf (%subscription-in-flight subscription)
                        (%make-subscription-delivery selected after))
                (progn
                  ;; Filtered events are acknowledged immediately.  They are
                  ;; outside the consumer's contract and must not block later
                  ;; matching events forever.
                  (save-subscription-offset
                   (%subscription-offset-store subscription)
                   (subscription-consumer-id subscription)
                   after)
                  (setf (subscription-cursor subscription) after)
                  nil))))))))

(defmethod subscription-ack ((subscription subscription) global-position)
  (%validate-durable-position global-position)
  (%with-durable-lock ((%subscription-lock subscription))
    (%ensure-subscription-lease-locked subscription)
    (let ((delivery (%subscription-in-flight subscription))
          (cursor (subscription-cursor subscription)))
      (cond
        (delivery
         (let ((expected
                 (subscription-delivery-after-global-position delivery)))
           (unless (= global-position expected)
             (error 'event-sourcing-error
                    :message "A subscription acknowledgement must cover its full delivery."))
           (save-subscription-offset
            (%subscription-offset-store subscription)
            (subscription-consumer-id subscription)
            global-position)
           (setf (subscription-cursor subscription) global-position
                 (%subscription-in-flight subscription) nil)
           global-position))
        ((<= global-position cursor) global-position)
        (t
         (error 'event-sourcing-error
                :message "A subscription acknowledgement is ahead of its cursor."))))))

(defmethod deliver-subscription ((subscription subscription) handler)
  (unless (functionp handler)
    (error 'type-error :datum handler :expected-type 'function))
  (let ((delivery (subscription-poll subscription)))
    (if (null delivery)
        (values nil 0)
      (progn
        (dolist (event (subscription-delivery-events delivery))
          (funcall handler event))
        (subscription-ack
         subscription
         (subscription-delivery-after-global-position delivery))
        (values delivery (length (subscription-delivery-events delivery)))))))
