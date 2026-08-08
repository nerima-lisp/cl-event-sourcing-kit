(defun %domain-event-equivalent-p (left right)
  (and
   (domain-event-p left)
   (domain-event-p right)
   (equal (domain-event-id left) (domain-event-id right))
   (equal (domain-event-type left) (domain-event-type right))
   (equal (domain-event-stream-id left) (domain-event-stream-id right))
   (equal (domain-event-aggregate-id left) (domain-event-aggregate-id right))
   (equal (domain-event-payload left) (domain-event-payload right))
   (equal (domain-event-metadata left) (domain-event-metadata right))
   (equal (domain-event-timestamp left) (domain-event-timestamp right))
   (equal
    (domain-event-correlation-id left)
    (domain-event-correlation-id right))
   (equal (domain-event-causation-id left) (domain-event-causation-id right))))

(defun %validate-expected-version (expected-version)
  (unless (or
           (eq expected-version :any)
           (eq expected-version :no-stream)
           (and (integerp expected-version) (<= 0 expected-version)))
    (error 'invalid-expected-version :value expected-version))
  expected-version)

(defun %expected-version-matches-p (expected-version exists-p actual-version)
  (cond
    ((eq expected-version :any) t)
    ((eq expected-version :no-stream) (not exists-p))
    (t (= expected-version actual-version))))

(defun %proper-list-p (object)
  (loop for cursor = object then (cdr cursor)
        do (cond
             ((null cursor) (return t))
             ((consp cursor))
             (t (return nil)))))

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
