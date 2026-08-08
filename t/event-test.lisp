(in-package #:cl-event-sourcing-kit/test)

(describe
 "domain event envelope"
 (it
  "keeps opaque payloads and metadata without choosing a format"
  (let* ((payload (list :opaque (make-hash-table)))
         (metadata (vector :metadata))
         (event
          (make-test-event
           "event-1"
           "stream-1"
           payload
           :metadata
           metadata
           :correlation-id
           "correlation-1"
           :causation-id
           "causation-1"
           :aggregate-id
           "aggregate-1")))
    (expect (domain-event-p event) :to-be-truthy)
    (expect (domain-event-id event) :to-be "event-1")
    (expect (domain-event-type event) :to-be :test-event)
    (expect (domain-event-stream-id event) :to-be "stream-1")
    (expect (domain-event-aggregate-id event) :to-be "aggregate-1")
    (expect (domain-event-payload event) :to-be payload)
    (expect (domain-event-metadata event) :to-be metadata)
    (expect (domain-event-timestamp event) :to-be 100)
    (expect (domain-event-version event) :to-be nil)
    (expect (domain-event-global-position event) :to-be nil)
    (expect (domain-event-correlation-id event) :to-be "correlation-1")
    (expect (domain-event-causation-id event) :to-be "causation-1")))
 (it
  "defaults the aggregate identity to the stream identity"
  (let ((event (make-test-event "event-1" "stream-1" nil)))
    (expect (domain-event-aggregate-id event) :to-be "stream-1")))
 (it
  "accepts injected id and clock functions"
  (let ((id-calls 0)
        (clock-calls 0))
    (let ((event
           (make-domain-event
            :type
            :generated
            :stream-id
            "stream-1"
            :payload
            nil
            :id-source
            (lambda ()
              (incf id-calls)
              "injected-id")
            :clock
            (lambda ()
              (incf clock-calls)
              987))))
      (expect (domain-event-id event) :to-be "injected-id")
      (expect (domain-event-timestamp event) :to-be 987)
      (expect id-calls :to-be 1)
      (expect clock-calls :to-be 1))))
 (it
  "allows explicit preassigned positions for adapter boundaries"
  (let ((event
         (make-test-event
          "event-1"
          "stream-1"
          :payload
          :version
          0
          :global-position
          0)))
    (expect (domain-event-version event) :to-be 0)
    (expect (domain-event-global-position event) :to-be 0)))
 (it
  "rejects missing identities and invalid positions"
  (signals
   invalid-domain-event
   (make-domain-event :stream-id "stream-1" :type nil))
  (signals invalid-domain-event (make-domain-event :type :event))
  (signals
   invalid-domain-event
   (make-domain-event :type :event :stream-id "stream-1" :version -1))
  (signals
   invalid-domain-event
   (make-domain-event :type :event :stream-id "stream-1" :global-position -1)))
 (it
  "does not expose mutable structure writers"
  (expect
   (fboundp
    (list 'setf 'domain-event-id))
   :to-be
   nil)
  (expect (fboundp 'copy-domain-event) :to-be nil)))
