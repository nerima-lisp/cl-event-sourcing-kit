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
    (expect
     (mapcar
      #'domain-event-payload
      (event-store-read store "stream-1" :to-version 1))
     :to-equal
     '(1))
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
         (different (make-test-event "event-1" "stream-1" :different))
         (schema-different
           (make-test-event
            "event-1"
            "stream-1"
            :original
            :schema-version
            2)))
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
     duplicate-event-id-conflict
     (event-store-append
      store
      "stream-1"
      (list schema-different)
      :expected-version
      1))
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
     (event-store-read store "stream-1"))
    (expect (event-store-retention-supported-p store) :to-be nil)
    (expect (event-store-retention-floor store) :to-be 0)
    (signals
     event-store-operation-not-supported
     (event-store-prune store :before-global-position 1))))
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

(describe
 "batch, snapshot, and identity contracts"
 (it
  "commits independent streams atomically and rolls back a later conflict"
  (let* ((store (make-event-store))
         (first-event (make-test-event "batch-a-1" "batch-a" :a))
         (second-event (make-test-event "batch-b-1" "batch-b" :b)))
    (let ((results
            (event-store-append-batch
             store
             (list
              (make-event-append-request
               :stream-id
               "batch-a"
               :events
               (list first-event)
               :expected-version
               :no-stream)
              (make-event-append-request
               :stream-id
               "batch-b"
               :events
               (list second-event)
               :expected-version
               :no-stream)))))
      (expect (length results) :to-be 2)
      (expect (event-append-result-version (first results)) :to-be 1)
      (expect (event-append-result-version (second results)) :to-be 1)
      (expect
       (mapcar
        #'domain-event-global-position
        (event-append-result-events (first results)))
       :to-equal
       '(1)))
    (let ((condition nil))
      (handler-case
          (event-store-append-batch
           store
           (list
            (make-event-append-request
             :stream-id
             "batch-c"
             :events
             (list (make-test-event "batch-c-1" "batch-c" :c))
             :expected-version
             :no-stream)
            (make-event-append-request
             :stream-id
             "batch-b"
             :events
             (list (make-test-event "batch-b-2" "batch-b" :new))
             :expected-version
             :no-stream)))
        (event-version-conflict (caught) (setf condition caught)))
      (expect condition :to-be-truthy))
    (expect (event-store-current-version store "batch-a") :to-be 1)
    (expect (event-store-current-version store "batch-b") :to-be 1)
    (expect (event-store-current-version store "batch-c") :to-be 0)
    (expect (event-store-stream-exists-p store "batch-c") :to-be nil)
    (expect (event-store-current-global-position store) :to-be 2)
    (expect (event-store-append-batch store nil) :to-be nil)
    (signals type-error (event-store-append-batch store (list :not-a-request)))
    (signals
     invalid-domain-event
     (event-store-append-batch
      store
      (list
       (cl-event-sourcing-kit::%make-event-append-request nil nil :any))))
    (signals invalid-snapshot (event-store-save-snapshot store :not-a-snapshot))))
 (it
  "rejects malformed snapshots and batch requests at construction time"
  (signals invalid-snapshot (make-event-snapshot))
  (signals invalid-snapshot
    (make-event-snapshot :stream-id nil :version 0))
  (signals invalid-snapshot
    (make-event-snapshot :stream-id "value-stream" :version :invalid))
  (signals invalid-domain-event
    (make-event-append-request :stream-id nil))
  (signals invalid-expected-version
    (make-event-append-request
     :stream-id "value-stream"
     :expected-version :invalid)))
 (it
  "saves, bounds, replaces, and deletes stream snapshot history"
  (let ((store (make-event-store)))
    (expect (event-store-snapshots-supported-p store) :to-be-truthy)
    (let ((empty
            (event-store-save-snapshot
             store
             (make-event-snapshot
              :stream-id
              "empty-stream"
              :version
              0
              :state
              :empty))))
      (expect (event-snapshot-version empty) :to-be 0)
      (expect
       (event-snapshot-state
        (event-store-read-snapshot store "empty-stream" :version 0))
       :to-be
       :empty))
    (event-store-append
     store
     "snapshot-stream"
     (list
      (make-test-event "snapshot-1" "snapshot-stream" 10)
      (make-test-event "snapshot-2" "snapshot-stream" 20))
     :expected-version
     :no-stream)
    (event-store-save-snapshot
     store
     (make-event-snapshot
      :stream-id
      "snapshot-stream"
      :version
      1
      :state
      10
      :metadata
      '(:source :test)))
    (expect
     (event-snapshot-state (event-store-read-snapshot
                            store
                            "snapshot-stream"
                            :version
                            1))
     :to-be
     10)
    (event-store-save-snapshot
     store
     (make-event-snapshot
      :stream-id
      "snapshot-stream"
      :version
      2
      :state
      30))
    (expect
     (event-snapshot-state (event-store-read-snapshot
                            store
                            "snapshot-stream"))
     :to-be
     30)
    (expect
     (event-snapshot-state
      (event-store-read-snapshot store "snapshot-stream" :version 1))
     :to-be
     10)
    (event-store-save-snapshot
     store
     (make-event-snapshot
      :stream-id
      "snapshot-stream"
      :version
      1
      :state
      :replacement))
    (expect
     (event-snapshot-state
      (event-store-read-snapshot store "snapshot-stream" :version 1))
     :to-be
     :replacement)
    (expect
     (event-snapshot-state
      (event-store-read-snapshot store "snapshot-stream" :version 2))
     :to-be
     30)
    (expect
     (event-store-read-snapshot store "snapshot-stream" :version 0)
     :to-be
     nil)
    (signals
     invalid-snapshot
     (event-store-save-snapshot
      store
      (make-event-snapshot
       :stream-id
       "snapshot-stream"
       :version
       3
       :state
       :future)))
    (signals
     invalid-snapshot
     (event-store-save-snapshot
      store
      (make-event-snapshot
       :stream-id
       "missing-stream"
       :version
       1
       :state
       :missing)))
    (signals
     invalid-snapshot
     (event-store-save-snapshot
      store
      (make-instance
       'event-snapshot
       :stream-id
       nil
       :version
       0
       :state
       :invalid-stream)))
    (signals
     invalid-snapshot
     (event-store-save-snapshot
      store
      (make-instance
       'event-snapshot
       :stream-id
       "snapshot-stream"
       :version
       :invalid
       :state
       :invalid-version)))
    (expect (event-store-delete-snapshot store "snapshot-stream")
            :to-be-truthy)
    (expect (event-store-read-snapshot store "snapshot-stream") :to-be nil)))
 (it
  "keeps mutable identities from corrupting the reference indexes"
  (let* ((store (make-event-store))
         (stream-id (copy-seq "mutable-stream"))
         (event-id (copy-seq "mutable-event"))
         (event (make-test-event event-id stream-id :payload)))
    (event-store-append store stream-id (list event) :expected-version :no-stream)
    (setf (char stream-id 0) #\M
          (char event-id 0) #\M)
    (expect (event-store-current-version store "mutable-stream") :to-be 1)
    (let ((stored-event (first (event-store-read store "mutable-stream"))))
      (expect (domain-event-aggregate-id stored-event)
              :to-equal
              "mutable-stream")
      (expect (length (event-store-read store "mutable-stream")) :to-be 1))
    (multiple-value-bind (committed version)
        (event-store-append
         store
         "mutable-stream"
         (list (make-test-event "mutable-event-2" "mutable-stream" :next))
         :expected-version
         1)
      (expect (length committed) :to-be 1)
      (expect version :to-be 2))
    (let ((cycle (cons :cycle nil)))
      (setf (cdr cycle) cycle)
      (signals
       invalid-domain-event
       (event-store-append
        store
        "cycle-stream"
        (list (make-test-event cycle "cycle-stream" :cycle))
        :expected-version
        :no-stream)))
    (signals invalid-domain-event (event-store-read store nil))
    (signals invalid-domain-event
      (event-store-current-version store nil))
    (signals invalid-domain-event (event-store-stream-exists-p store nil))
    (signals invalid-domain-event
      (event-store-read-snapshot store nil))
    (signals invalid-domain-event
      (event-store-delete-snapshot store nil))
    (signals type-error (make-in-memory-event-store :lock 'not-a-lock))))
 (it
  "rejects cyclic event batches before traversing them"
  (let* ((store (make-event-store))
         (event (make-test-event "cyclic-batch-event" "cyclic-batch" :payload))
         (events (list event)))
    (setf (cdr events) events)
    (signals
     invalid-domain-event
     (event-store-append
      store
      "cyclic-batch"
      events
      :expected-version
      :no-stream))
    (expect (event-store-current-version store "cyclic-batch") :to-be 0)))
 (it
  "compares cyclic payload and metadata safely during idempotent retries"
  (let* ((store (make-event-store))
         (payload-a (cons :payload nil))
         (payload-b (cons :payload nil))
         (metadata-a (cons :metadata nil))
         (metadata-b (cons :metadata nil))
         (original
           (make-test-event
            "cyclic-retry-event"
            "cyclic-retry"
            payload-a
            :metadata
            metadata-a))
         (retry
           (make-test-event
            "cyclic-retry-event"
            "cyclic-retry"
            payload-b
            :metadata
            metadata-b)))
    (setf (cdr payload-a) payload-a
          (cdr payload-b) payload-b
          (cdr metadata-a) metadata-a
          (cdr metadata-b) metadata-b)
    (event-store-append
     store
     "cyclic-retry"
     (list original)
     :expected-version
     :no-stream)
    (multiple-value-bind (committed version)
        (event-store-append
         store
         "cyclic-retry"
         (list retry)
         :expected-version
         :any)
      (expect (first committed) :to-be (first (event-store-read store "cyclic-retry")))
      (expect version :to-be 1)
      (expect (event-store-current-global-position store) :to-be 1))))
 (it
  "rejects mutable array identities in the reference store"
  (let* ((store (make-event-store))
         (stream-id (vector :mutable-stream)))
    (signals
     invalid-domain-event
     (event-store-append
      store
      stream-id
      (list (make-test-event "array-identity-event" stream-id :payload))
      :expected-version
      :no-stream))))
 (it
  "rejects cyclic stream identities without recursive equality"
  (let* ((store (make-event-store))
         (requested-stream-id (cons :cyclic-stream nil))
         (event-stream-id (cons :cyclic-stream nil)))
    (setf (cdr requested-stream-id) requested-stream-id
          (cdr event-stream-id) event-stream-id)
    (signals
     invalid-domain-event
     (event-store-append
      store
      requested-stream-id
      (list (make-test-event
             "cyclic-stream-event"
             event-stream-id
             :payload))
      :expected-version
      :no-stream))))
 (it
  "handles nested and opaque stream identities with legacy version fallback"
  (let ((store (make-event-store))
        (nested-stream (list "nested-stream" :opaque)))
    (event-store-append
     store
     nested-stream
     (list (make-test-event
            "nested-identity-event"
            nested-stream
            :payload))
     :expected-version
     :no-stream)
    (expect (event-store-current-version
             store
             (list "nested-stream" :opaque))
            :to-be
            1)
    (signals
     invalid-domain-event
     (cl-event-sourcing-kit::%in-memory-identity-key
      (list "nested-stream" (vector :mutable))))
    (signals
     invalid-domain-event
     (event-store-append
      store
      (list "nested-stream" (vector :mutable))
      (list (make-test-event
             "nested-array-event"
             (list "nested-stream" (vector :mutable))
             :payload))
      :expected-version
      :no-stream))
    (event-store-append
     store
     :opaque-stream
     (list (make-test-event
            "opaque-stream-event"
            :opaque-stream
            :payload))
     :expected-version
     :no-stream)
    (expect (event-store-current-version store :opaque-stream)
            :to-be
            1)
    (event-store-append
     store
     "legacy-stream"
     (list (make-test-event
            "legacy-stream-event"
            "legacy-stream"
            :payload))
     :expected-version
     :no-stream)
    (remhash
     (cl-event-sourcing-kit::%in-memory-identity-key "legacy-stream")
     (cl-event-sourcing-kit::%in-memory-stream-versions store))
    (expect (event-store-current-version store "legacy-stream")
            :to-be
            1)))
 (it
  "rejects the same event id when its stream content differs"
  (let ((store (make-event-store)))
    (event-store-append
     store
     "stream-a"
     (list (make-test-event "shared-id" "stream-a" :a))
     :expected-version
     :no-stream)
    (signals
     duplicate-event-id-conflict
     (event-store-append
      store
      "stream-b"
      (list (make-test-event "shared-id" "stream-b" :b))
      :expected-version
      :no-stream))
    (expect (event-store-current-version store "stream-b") :to-be 0))))
