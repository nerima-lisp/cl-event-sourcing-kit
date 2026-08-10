;;;; Reproducible in-memory performance probes.
;;;;
;;;; Run with:
;;;;   nix develop -c sbcl --script bench/performance.lisp
;;;;
;;;; CL_EVENT_SOURCING_KIT_BENCH_COUNT controls the append workload.

(require :asdf)

(defun %benchmark-script-directory ()
  (make-pathname :name nil
                 :type nil
                 :defaults (or *load-truename*
                               *compile-file-truename*
                               (error "Unable to determine the benchmark path."))))

(defun %benchmark-repository-root ()
  (truename
   (merge-pathnames
    "../"
    (%benchmark-script-directory))))

(asdf:initialize-source-registry
 `(:source-registry
   (:tree ,(%benchmark-repository-root))
   :inherit-configuration))
(asdf:load-system "cl-event-sourcing-kit/durable")

(defpackage #:cl-event-sourcing-kit/benchmark
  (:use #:cl #:cl-event-sourcing-kit))

(in-package #:cl-event-sourcing-kit/benchmark)

(defparameter *benchmark-stream-id* "performance-stream")

(defun %benchmark-count ()
  (let ((value (uiop:getenv "CL_EVENT_SOURCING_KIT_BENCH_COUNT")))
    (if value
        (parse-integer value)
      5000)))

(defun %collect-garbage ()
  #+sbcl (sb-ext:gc :full t)
  #-sbcl nil)

(defun %measure (label expected-count thunk)
  (%collect-garbage)
  (let* ((start (get-internal-real-time))
         (observed-count (funcall thunk))
         (elapsed-ticks (max 1 (- (get-internal-real-time) start)))
         (elapsed-seconds
           (/ (float elapsed-ticks 1.0d0)
              internal-time-units-per-second)))
    (unless (= observed-count expected-count)
      (error "~A observed ~D items; expected ~D."
             label
             observed-count
             expected-count))
    (format t
            "~&~A count=~D seconds=~,6F operations/second=~,1F~%"
            label
            observed-count
            elapsed-seconds
            (/ expected-count elapsed-seconds))
    observed-count))

(defun %benchmark-event (id)
  (make-domain-event
   :id id
   :type :performance-event
   :stream-id *benchmark-stream-id*
   :payload id
   :timestamp 0
   :schema-version 1))

(defun %benchmark-outbox-message (id)
  (make-outbox-message
   :id id
   :topic :performance-topic
   :payload id
   :created-at 0
   :available-at 0))

(defun %assert-ordered-values (label items expected-count accessor)
  (unless (= (length items) expected-count)
    (error "~A returned ~D items; expected ~D."
           label
           (length items)
           expected-count))
  (loop for item in items
        for expected from 0
        unless (= (funcall accessor item) expected)
          do (error "~A returned item ~D at position ~D."
                    label
                    (funcall accessor item)
                    expected))
  t)

(defun %assert-event-store-count (store count)
  (%assert-ordered-values
   "event stream"
   (event-store-read store *benchmark-stream-id*)
   count
   #'domain-event-id)
  (%assert-ordered-values
   "global event feed"
   (event-store-read-all store)
   count
   #'domain-event-id))

(defun %assert-outbox-count (store count)
  (%assert-ordered-values
   "outbox"
   (outbox-read-all store)
   count
   #'outbox-message-id))

(defun %warm-up-methods ()
  (let ((event-store (make-in-memory-event-store))
        (outbox-store (make-in-memory-outbox-store)))
    (event-store-append
     event-store
     *benchmark-stream-id*
     (list (%benchmark-event 0))
     :expected-version 0)
    (event-store-current-version event-store *benchmark-stream-id*)
    (event-store-read event-store *benchmark-stream-id*)
    (event-store-read-all event-store)
    (event-store-read-all event-store :limit 1)
    (outbox-append outbox-store (%benchmark-outbox-message 0))
    (outbox-read-all outbox-store)
    (outbox-read-all outbox-store :limit 1)
    (outbox-read-pending outbox-store :now 0 :limit 1)
    (let ((message (first (outbox-claim outbox-store
                                        :now 0
                                        :limit 1
                                        :lease-seconds 60))))
      (outbox-ack outbox-store
                  (outbox-message-id message)
                  :claim-token (outbox-message-claim-token message)))
    t))

(defun %run-event-store-benchmarks (count)
  (let ((store (make-in-memory-event-store)))
    (%measure
     "event-store-append/single"
     count
     (lambda ()
       (dotimes (id count)
         (event-store-append
          store
          *benchmark-stream-id*
          (list (%benchmark-event id))
          :expected-version id))
       (event-store-current-version store *benchmark-stream-id*)))
    (%assert-event-store-count store count)
    (%measure
     "event-store-read/full"
     count
     (lambda ()
       (length (event-store-read store *benchmark-stream-id*))))
    (%measure
     "event-store-read-all/full"
     count
     (lambda ()
       (length (event-store-read-all store))))
    (%measure
     "event-store-read-all/limited"
     (min count 64)
     (lambda ()
       (let ((limited (event-store-read-all store :limit (min count 64))))
         (%assert-ordered-values
          "limited global event feed"
          limited
          (min count 64)
          #'domain-event-id)
         (length limited))))
    t))

(defun %run-outbox-benchmarks (count)
  (let ((store (make-in-memory-outbox-store)))
    (%measure
     "outbox-append/single"
     count
     (lambda ()
       (dotimes (id count)
         (outbox-append store (%benchmark-outbox-message id)))
       count))
    (%assert-outbox-count store count)
    (%measure
     "outbox-read-all/full"
     count
     (lambda ()
       (length (outbox-read-all store))))
    (%measure
     "outbox-read-all/limited"
     (min count 64)
     (lambda ()
       (let ((limited (outbox-read-all store :limit (min count 64))))
         (%assert-ordered-values
          "limited outbox"
          limited
          (min count 64)
          #'outbox-message-id)
         (length limited))))
    (%measure
     "outbox-read-pending/limited"
     (min count 64)
     (lambda ()
       (length
        (outbox-read-pending store :now 0 :limit (min count 64)))))
    t))

(defun %run-outbox-lifecycle-benchmark (count)
  (let ((store (make-in-memory-outbox-store))
        (claimed nil))
    (dotimes (id count)
      (outbox-append store (%benchmark-outbox-message id)))
    (%assert-outbox-count store count)
    (%measure
     "outbox-claim/all"
     count
     (lambda ()
       (setf claimed
             (outbox-claim store :now 0 :limit count :lease-seconds 60))
       (length claimed)))
    (%assert-ordered-values
     "claimed outbox"
     claimed
     count
     #'outbox-message-id)
    (%measure
     "outbox-ack/single"
     count
     (lambda ()
       (let ((acked 0))
         (dolist (message claimed acked)
           (outbox-ack store
                       (outbox-message-id message)
                       :claim-token (outbox-message-claim-token message))
           (incf acked)))))
    (%assert-ordered-values
     "delivered outbox"
     (outbox-read-all store :status :delivered)
     count
     #'outbox-message-id)
    t))

(defun %run-outbox-limited-claim-benchmark (count)
  (let ((store (make-in-memory-outbox-store))
        (claimed nil)
        (limit (min count 64)))
    (dotimes (id count)
      (outbox-append store (%benchmark-outbox-message id)))
    (%assert-outbox-count store count)
    (%measure
     "outbox-claim/limited"
     limit
     (lambda ()
       (setf claimed
             (outbox-claim store :now 0 :limit limit :lease-seconds 60))
       (length claimed)))
    (%assert-ordered-values
     "limited claimed outbox"
     claimed
     limit
     #'outbox-message-id)
    t))

(let ((count (%benchmark-count)))
  (unless (plusp count)
    (error "CL_EVENT_SOURCING_KIT_BENCH_COUNT must be positive."))
  (format t "~&benchmark-count=~D~%" count)
  (%warm-up-methods)
  (%run-event-store-benchmarks count)
  (%run-outbox-benchmarks count)
  (%run-outbox-limited-claim-benchmark count)
  (%run-outbox-lifecycle-benchmark count))
