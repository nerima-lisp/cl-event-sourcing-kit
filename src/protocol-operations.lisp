(defun %safe-equal-p (left right)
  "Compare opaque values without recursing forever through cyclic conses.

Common Lisp's EQUAL has no portable cycle guarantee.  Event payloads and
metadata are deliberately opaque, so the core must preserve that contract
without allowing a duplicate-id retry to hang on a cyclic cons tree."
  (labels ((compare (left right seen)
             (cond
               ((eq left right) t)
               ((and (consp left) (consp right))
                (let ((matched-rights (gethash left seen)))
                  (if (member right matched-rights :test #'eq)
                      t
                    (progn
                      (setf (gethash left seen)
                            (cons right matched-rights))
                      (and (compare (car left) (car right) seen)
                           (compare (cdr left) (cdr right) seen))))))
               (t (equal left right)))))
    (compare left right (make-hash-table :test #'eq))))

(defmethod event-store-supports-p (store capability)
  (unless (keywordp capability)
    (error 'type-error :datum capability :expected-type 'keyword))
  (not (null (member capability
                     (event-store-capabilities store)
                     :test #'eq))))

(defun event-store-require-capabilities (store capabilities)
  "Return STORE when every keyword in CAPABILITIES is advertised.

Signal EVENT-STORE-OPERATION-NOT-SUPPORTED with all missing capabilities as
the operation payload.  Failing before a write or a delivery loop starts is
important for portable applications: an adapter must not silently downgrade
an operation whose correctness depends on durability, fencing, or atomicity."
  (unless (%proper-list-p capabilities)
    (error 'type-error :datum capabilities :expected-type 'list))
  (let ((missing nil))
    (dolist (capability capabilities)
      (unless (keywordp capability)
        (error 'type-error :datum capability :expected-type 'keyword))
      (unless (event-store-supports-p store capability)
        (pushnew capability missing :test #'eq)))
    (when missing
      (error 'event-store-operation-not-supported
             :operation (list :required-capabilities (nreverse missing))
             :store store))
    store))

(defun %domain-event-equivalent-p (left right)
  (and
   (domain-event-p left)
   (domain-event-p right)
   (%safe-equal-p (domain-event-id left) (domain-event-id right))
   (%safe-equal-p (domain-event-type left) (domain-event-type right))
   (%safe-equal-p (domain-event-stream-id left) (domain-event-stream-id right))
   (%safe-equal-p (domain-event-aggregate-id left) (domain-event-aggregate-id right))
   (%safe-equal-p (domain-event-payload left) (domain-event-payload right))
   (%safe-equal-p (domain-event-metadata left) (domain-event-metadata right))
   (%safe-equal-p (domain-event-timestamp left) (domain-event-timestamp right))
   (= (domain-event-schema-version left)
      (domain-event-schema-version right))
   (%safe-equal-p
    (domain-event-correlation-id left)
    (domain-event-correlation-id right))
   (%safe-equal-p
    (domain-event-causation-id left)
    (domain-event-causation-id right))))

(defun %validate-expected-version (expected-version)
  (unless (%valid-expected-version-p expected-version)
    (error 'invalid-expected-version :value expected-version))
  expected-version)

(defun %validate-event-snapshot (snapshot &key (stream-id *unspecified*))
  (unless (event-snapshot-p snapshot)
    (error
     'invalid-snapshot
     :stream-id (if (eq stream-id *unspecified*) nil stream-id)
     :version nil
     :reason :snapshot-type))
  (let ((snapshot-stream-id (event-snapshot-stream-id snapshot))
        (version (event-snapshot-version snapshot)))
    (when (or (null snapshot-stream-id) (eq snapshot-stream-id *unspecified*))
      (error
       'invalid-snapshot
       :stream-id snapshot-stream-id
       :version version
       :reason :stream-id))
    (unless (and (integerp version) (<= 0 version))
      (error
       'invalid-snapshot
       :stream-id snapshot-stream-id
       :version version
       :reason :version))
    (when (and (not (eq stream-id *unspecified*))
               (not (%safe-equal-p stream-id snapshot-stream-id)))
      (error
       'invalid-snapshot
       :stream-id snapshot-stream-id
       :version version
       :reason :stream-id)))
  snapshot)

(defun %expected-version-matches-p (expected-version exists-p actual-version)
  (cond
    ((eq expected-version :any) t)
    ((eq expected-version :no-stream) (not exists-p))
    (t (= expected-version actual-version))))

(defun %proper-list-p (object)
  (let ((seen (make-hash-table :test #'eq)))
    (loop for cursor = object then (cdr cursor)
          do (cond
               ((null cursor) (return t))
               ((not (consp cursor)) (return nil))
               ((gethash cursor seen) (return nil))
               (t (setf (gethash cursor seen) t))))))

(defun %coerce-event-list (events)
  (cond
    ((null events) nil)
    ((vectorp events)
     (loop for event across events
           collect event))
    ((%proper-list-p events) (copy-list events))
    (t
     (%invalid-domain-event
      events
      :events
      "EVENTS must be a proper list or vector."))))

(defun %signal-version-conflict (stream-id expected-version actual-version)
  (error
   'event-version-conflict
   :stream-id
   stream-id
   :expected-version
   expected-version
   :actual-version
   actual-version))
