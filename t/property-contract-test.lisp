(in-package #:cl-event-sourcing-kit/test)

(it-property
 "generated append traces preserve stream order and CPS reads"
 ((payloads (gen-list (gen-integer :min -20 :max 20)
                      :min-length 1
                      :max-length 8)))
 (let* ((store (make-event-store))
        (events
          (loop for payload in payloads
                for index from 1
                collect (make-test-event
                         (format nil "property-event-~D" index)
                         "property-stream"
                         payload)))
        (expected-versions
          (loop for index from 1 to (length payloads)
                collect index)))
   (event-store-append store
                       "property-stream"
                       events
                       :expected-version
                       :no-stream)
   (with-continuation-result (read-events next calledp)
       (event-store-read/cc store "property-stream" #'next)
     (expect calledp :to-be-truthy)
     (expect (mapcar #'domain-event-payload read-events)
             :to-equal
             payloads)
     (expect (mapcar #'domain-event-version read-events)
             :to-equal
             expected-versions)
     (expect (event-store-current-version store "property-stream")
             :to-be
             (length payloads)))))
