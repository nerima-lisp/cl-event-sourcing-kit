(in-package #:cl-event-sourcing-kit/test)

(describe
 "event store protocol and in-memory reference"
 (it
  "appends in stream order and reads inclusive version ranges"
  (let ((store (make-event-store)))
    (multiple-value-bind (events version) (event-store-append
                                           store
                                           "stream-1"
                                           (vector
                                            (make-test-event
                                             "event-1"
                                             "stream-1"
                                             1)
                                            (make-test-event
                                             "event-2"
                                             "stream-1"
                                             2))
                                           :expected-version
                                           :no-stream)
      (expect (mapcar #'domain-event-version events) :to-equal '(1 2))
      (expect version :to-be 2))
    (expect (event-store-stream-exists-p store "stream-1") :to-be-truthy)
    (expect (event-store-current-version store "stream-1") :to-be 2)
    (expect (event-store-current-version store "missing") :to-be 0)
    (expect
     (mapcar
      #'domain-event-payload
      (event-store-read store "stream-1" :from-version 2 :to-version 2))
     :to-equal
     '(2))
    (expect (event-store-read store "missing") :to-be nil)))
 (it
  "implements no-stream, any, and exact expected versions"
  (let ((store (make-event-store)))
    (event-store-append
     store
     "stream-1"
     (list (make-test-event "event-1" "stream-1" 1))
     :expected-version
     0)
    (event-store-append
     store
     "stream-1"
     (list (make-test-event "event-2" "stream-1" 2))
     :expected-version
     :any)
    (signals
     event-version-conflict
     (event-store-append
      store
      "stream-1"
      (list (make-test-event "event-3" "stream-1" 3))
      :expected-version
      :no-stream))
    (signals
     event-version-conflict
     (event-store-append
      store
      "stream-1"
      (list (make-test-event "event-4" "stream-1" 4))
      :expected-version
      1))
    (expect (event-store-current-version store "stream-1") :to-be 2)))
 (it
  "reports conflict details and leaves the append atomic"
  (let ((store (make-event-store))
        (condition nil))
    (event-store-append
     store
     "stream-1"
     (list (make-test-event "event-1" "stream-1" 1))
     :expected-version
     :no-stream)
    (handler-case (event-store-append
                   store
                   "stream-1"
                   (list
                    (make-test-event "event-2" "stream-1" 2)
                    (make-test-event "event-3" "stream-1" 3))
                   :expected-version
                   0)
      (event-version-conflict (caught)
        (setf condition caught)))
    (expect (event-version-conflict-stream-id condition) :to-be "stream-1")
    (expect (event-version-conflict-expected-version condition) :to-be 0)
    (expect (event-version-conflict-actual-version condition) :to-be 1)
    (expect (event-store-current-version store "stream-1") :to-be 1)
    (expect (length (event-store-read store "stream-1")) :to-be 1)))
 (it
  "treats an all-duplicate retry as idempotent"
  (let* ((store (make-event-store))
         (requested (make-test-event "event-1" "stream-1" :payload)))
    (multiple-value-bind (committed version) (event-store-append
                                              store
                                              "stream-1"
                                              (list requested)
                                              :expected-version
                                              :no-stream)
      (multiple-value-bind (retry retry-version) (event-store-append
                                                  store
                                                  "stream-1"
                                                  (list requested)
                                                  :expected-version
                                                  0)
        (expect (first retry) :to-be (first committed))
        (expect retry-version :to-be version)
        (expect (event-store-current-global-position store) :to-be 1)))))
 (it
  "rejects mismatched and repeated ids without partial writes"
  (let* ((store (make-event-store))
         (original (make-test-event "event-1" "stream-1" :original))
         (different (make-test-event "event-1" "stream-1" :different)))
    (event-store-append
     store
     "stream-1"
     (list original)
     :expected-version
     :no-stream)
    (signals
     duplicate-event-id-conflict
     (event-store-append store "stream-1" (list different) :expected-version 1))
    (signals
     duplicate-event-id
     (event-store-append
      store
      "stream-1"
      (list
       (make-test-event "event-2" "stream-1" 2)
       (make-test-event "event-2" "stream-1" 2))
      :expected-version
      1))
    (expect (event-store-current-version store "stream-1") :to-be 1)))
 (it
  "checks expected version when a retry also contains new events"
  (let* ((store (make-event-store))
         (first-event (make-test-event "event-1" "stream-1" 1))
         (second-event (make-test-event "event-2" "stream-1" 2)))
    (event-store-append
     store
     "stream-1"
     (list first-event)
     :expected-version
     :no-stream)
    (signals
     event-version-conflict
     (event-store-append
      store
      "stream-1"
      (list first-event second-event)
      :expected-version
      0))
    (event-store-append
     store
     "stream-1"
     (list first-event second-event)
     :expected-version
     1)
    (expect (event-store-current-version store "stream-1") :to-be 2)))
 (it
  "preserves global ordering across streams and supports cursors"
  (let ((store (make-event-store)))
    (event-store-append
     store
     "stream-b"
     (list (make-test-event "event-b" "stream-b" :b))
     :expected-version
     :no-stream)
    (event-store-append
     store
     "stream-a"
     (list (make-test-event "event-a" "stream-a" :a))
     :expected-version
     :no-stream)
    (expect
     (mapcar #'domain-event-id (event-store-read-all store))
     :to-equal
     '("event-b" "event-a"))
    (expect
     (mapcar
      #'domain-event-id
      (event-store-read-all store :after-global-position 1 :limit 1))
     :to-equal
     '("event-a"))
    (expect (event-store-current-global-position store) :to-be 2)
    (expect (event-store-global-position-supported-p store) :to-be-truthy)))
 (it
  "supports an adapter by subclassing the protocol directly"
  (let* ((store (make-instance 'protocol-store))
         (event (make-test-event "event-1" "stream-1" :payload)))
    (multiple-value-bind (events version) (event-store-append
                                           store
                                           "stream-1"
                                           (list event)
                                           :expected-version
                                           :no-stream)
      (expect events :to-equal (list event))
      (expect version :to-be 1))
    (signals
     event-store-operation-not-supported
     (event-store-read store "stream-1"))))
 (it
  "reports unsupported adapter operations structurally"
  (let ((condition nil)
        (store (make-instance 'unsupported-store)))
    (handler-case (event-store-read store "stream-1")
      (event-store-operation-not-supported (caught)
        (setf condition caught)))
    (expect condition :to-be-truthy)
    (expect (event-store-operation condition) :to-be :read)
    (expect (event-store-operation-store condition) :to-be store)))
 (it
  "compares event content while ignoring assigned positions"
  (let* ((store (make-event-store))
         (stored
          (make-test-event
           "event-1"
           "stream-1"
           :payload
           :version
           1
           :global-position
           1))
         (requested (make-test-event "event-1" "stream-1" :payload)))
    (expect
     (event-store-event-equivalent-p store stored requested)
     :to-be-truthy)
    (expect
     (event-store-event-equivalent-p
      store
      stored
      (make-test-event "event-1" "stream-1" :different))
     :to-be
     nil))))
