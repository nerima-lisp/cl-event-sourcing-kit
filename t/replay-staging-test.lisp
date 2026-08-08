(in-package #:cl-event-sourcing-kit/test)

(describe
 "replay and uncommitted staging"
 (it
  "folds events left to right as a pure operation"
  (let ((events
         (list
          (make-test-event "event-1" "stream-1" 2)
          (make-test-event "event-2" "stream-1" 3))))
    (expect
     (replay-events
      10
      events
      (lambda (state event)
        (+ state (domain-event-payload event))))
     :to-be
     15)
    (expect
     (replay-events
      nil
      events
      (lambda (state event)
        (declare (ignore event))
        state))
     :to-be
     nil)))
 (it
  "stages one stream and clears only after a successful commit"
  (let* ((store (make-event-store))
         (first-event (make-test-event "event-1" "stream-1" :first))
         (second-event (make-test-event "event-2" "stream-1" :second)))
    (with-event-staging
     (staging "stream-1")
     (stage-event staging first-event)
     (stage-event staging second-event)
     (expect
      (uncommitted-events staging)
      :to-equal
      (list first-event second-event))
     (multiple-value-bind (committed version) (commit-events staging store)
       (expect (length committed) :to-be 2)
       (expect version :to-be 2))
     (expect (uncommitted-events staging) :to-be nil)
     (expect (event-staging-expected-version staging) :to-be 2))))
 (it
  "retains staged events after an optimistic conflict"
  (let* ((store (make-event-store))
         (staging (make-event-staging "stream-1" :expected-version 0))
         (staged (make-test-event "staged" "stream-1" :staged))
         (other (make-test-event "other" "stream-1" :other)))
    (stage-event staging staged)
    (event-store-append
     store
     "stream-1"
     (list other)
     :expected-version
     :no-stream)
    (signals event-version-conflict (commit-events staging store))
    (expect (uncommitted-events staging) :to-equal (list staged))
    (expect (event-staging-expected-version staging) :to-be 0)
    (commit-events staging store :expected-version 1)
    (expect (uncommitted-events staging) :to-be nil)))
 (it
  "rejects events from another stream or with assigned positions"
  (let ((staging (make-event-staging "stream-1")))
    (signals
     invalid-domain-event
     (stage-event staging (make-test-event "event-1" "stream-2" :wrong)))
    (signals
     invalid-domain-event
     (stage-event
      staging
      (make-test-event "event-2" "stream-1" :version-0 :version 0)))
    (signals
     invalid-domain-event
     (stage-event
      staging
      (make-test-event "event-3" "stream-1" :global-0 :global-position 0)))))
 (it
  "rejects non-event input and invalid reducer arguments"
  (signals
   invalid-domain-event
   (stage-event (make-event-staging "stream-1") :not-an-event))
  (signals type-error (replay-events nil nil nil)))
 (it
  "validates staging construction and supports explicit append expectations"
  (signals invalid-domain-event (make-event-staging nil))
  (signals
   invalid-expected-version
   (make-event-staging "stream-1" :expected-version :invalid))
  (let ((store (make-event-store))
        (staging (make-event-staging "stream-1" :expected-version :any)))
    (stage-event staging (make-test-event "event-1" "stream-1" 1))
    (commit-events staging store :expected-version :no-stream)
    (expect (event-staging-expected-version staging) :to-be 1))))
