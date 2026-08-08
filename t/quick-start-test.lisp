(in-package #:cl-event-sourcing-kit/test)

(describe
 "public API quick start"
 (it
  "creates, appends, reads, and replays an event without internals"
  (let* ((store (make-event-store))
         (event
          (make-domain-event
           :id
           "quick-start-1"
           :type
           :value-added
           :stream-id
           "quick-stream"
           :payload
           42
           :metadata
           '(:test t)
           :timestamp
           1)))
    (multiple-value-bind (committed version) (event-store-append
                                              store
                                              "quick-stream"
                                              (list event)
                                              :expected-version
                                              :no-stream)
      (expect version :to-be 1)
      (expect (domain-event-version (first committed)) :to-be 1))
    (let ((events (event-store-read store "quick-stream")))
      (expect (length events) :to-be 1)
      (expect
       (replay-events
        0
        events
        (lambda (state current)
          (+ state (domain-event-payload current))))
       :to-be
       42)))))
