(in-package #:cl-event-sourcing-kit/test)

(defclass unsupported-lease-store (subscription-lease-store)
  ())

(describe
 "subscription lease contracts"
 (it
  "validates lease values and store boundaries"
  (let ((lease
          (make-subscription-lease
           :consumer-id "consumer"
           :owner-id "owner"
           :fencing-token 1
           :expires-at 10)))
    (expect (subscription-lease-p lease) :to-be-truthy)
    (expect (subscription-lease-consumer-id lease) :to-equal "consumer")
    (expect (subscription-lease-owner-id lease) :to-equal "owner")
    (expect (subscription-lease-fencing-token lease) :to-be 1)
    (expect (subscription-lease-expires-at lease) :to-be 10)
    (signals event-sourcing-error
      (make-subscription-lease))
    (signals event-sourcing-error
      (make-subscription-lease :consumer-id nil
                                :owner-id "owner"
                                :fencing-token 1
                                :expires-at 10))
    (signals event-sourcing-error
      (make-subscription-lease :consumer-id "consumer"
                                :owner-id nil
                                :fencing-token 1
                                :expires-at 10))
    (signals type-error
      (make-subscription-lease :consumer-id "consumer"
                                :owner-id "owner"
                                :fencing-token 0
                                :expires-at 10))
    (signals type-error
      (make-subscription-lease :consumer-id "consumer"
                                :owner-id "owner"
                                :fencing-token "not-a-token"
                                :expires-at 10))
    (signals type-error
      (make-subscription-lease :consumer-id "consumer"
                                :owner-id "owner"
                                :fencing-token 1
                                :expires-at "not-a-time"))
    (signals type-error
      (subscription-lease-valid-p (make-instance 'unsupported-lease-store)
                                  1
                                  :now 0))
    (let ((store (make-instance 'unsupported-lease-store)))
      (expect (subscription-lease-store-p store) :to-be-truthy)
      (expect (subscription-lease-store-capabilities store) :to-be nil)
      (signals event-store-operation-not-supported
        (subscription-lease-acquire store "consumer" "owner" :now 0))
      (signals event-store-operation-not-supported
        (subscription-lease-renew store lease :now 0))
      (signals event-store-operation-not-supported
        (subscription-lease-release store lease))
      (expect (subscription-lease-valid-p store lease :now 0)
              :to-be
              nil))))

 (it
  "provides atomic in-memory ownership, renewal, expiry, and fencing"
  (let* ((store (make-in-memory-subscription-lease-store))
         (first (subscription-lease-acquire
                 store
                 "consumer"
                 "owner-a"
                 :now 0
                 :lease-seconds 10)))
    (expect (in-memory-subscription-lease-store-p store) :to-be-truthy)
    (expect (subscription-lease-store-capabilities store)
            :to-equal
            '(:consumer-leases :fencing :process-local-lock))
    (let ((default-lease
            (subscription-lease-acquire
             store
             "default-consumer"
             "owner"
             :now 0)))
      (expect (subscription-lease-expires-at default-lease) :to-be 60)
      (expect (subscription-lease-release store default-lease) :to-be-truthy))
    (let ((applied-default-lease
            (apply #'subscription-lease-acquire
                   (list store "applied-default-consumer" "owner" :now 0))))
      (expect (subscription-lease-expires-at applied-default-lease) :to-be 60)
      (expect (subscription-lease-release store applied-default-lease)
              :to-be-truthy))
    (expect (subscription-lease-fencing-token first) :to-be 1)
    (expect (subscription-lease-expires-at first) :to-be 10)
    (let ((renewed
            (subscription-lease-acquire
             store
             "consumer"
             "owner-a"
             :now 5
             :lease-seconds 20)))
      (expect (subscription-lease-fencing-token renewed) :to-be 1)
      (expect (subscription-lease-expires-at renewed) :to-be 25)
      (expect (subscription-lease-acquire
               store
               "consumer"
               "owner-b"
               :now 6
               :lease-seconds 10)
              :to-be
              nil)
      (expect (subscription-lease-valid-p store renewed :now 24)
              :to-be-truthy)
      (expect (subscription-lease-valid-p store renewed :now 25)
              :to-be
              nil)
      (let ((renewed-again
              (subscription-lease-renew
               store
               renewed
               :now 24
               :lease-seconds 10)))
        (expect (subscription-lease-fencing-token renewed-again) :to-be 1)
        (expect (subscription-lease-expires-at renewed-again) :to-be 34))
      (expect (subscription-lease-release store first) :to-be-truthy)
      (expect (subscription-lease-release store renewed) :to-be nil)
      (let ((second
              (subscription-lease-acquire
               store
               "consumer"
               "owner-b"
               :now 100
               :lease-seconds 10)))
        (expect (subscription-lease-fencing-token second) :to-be 2)
        (let ((second-renewed
                (subscription-lease-renew store second :now 109)))
          (expect (subscription-lease-fencing-token second-renewed)
                  :to-be
                  2)
          (expect (subscription-lease-expires-at second-renewed)
                  :to-be
                  169))
        (expect (subscription-lease-renew store renewed :now 110)
                :to-be
                nil)
        (expect (subscription-lease-release store renewed) :to-be nil)
        (expect (subscription-lease-valid-p store second :now 168)
                :to-be-truthy)
        (expect (subscription-lease-valid-p store first :now 168)
                :to-be
                nil)
        (expect (subscription-lease-valid-p store second :now 169)
                :to-be
                nil)
        (expect (subscription-lease-release store second) :to-be-truthy)
        (expect (subscription-lease-release store second) :to-be nil)))
    (let* ((clock (get-universal-time))
           (default-store (make-in-memory-subscription-lease-store))
           (default-lease
             (subscription-lease-acquire
              default-store
              "default-renew-consumer"
              "owner"
              :now clock
              :lease-seconds 100))
           (renewed (apply #'subscription-lease-renew
                          (list default-store default-lease))))
      (expect renewed :to-be-truthy)
      (expect (> (subscription-lease-expires-at renewed) (+ clock 50))
              :to-be-truthy)
      (expect (subscription-lease-valid-p default-store renewed)
              :to-be-truthy))
    (let* ((expired-store (make-in-memory-subscription-lease-store))
           (expired-lease
             (subscription-lease-acquire
              expired-store
              "expired-consumer"
              "owner"
              :now 0
              :lease-seconds 1)))
      (expect (subscription-lease-renew
               expired-store
               expired-lease
               :now 1
               :lease-seconds 10)
              :to-be
              nil))
    (signals event-sourcing-error
      (subscription-lease-acquire store nil "owner" :now 0))
    (signals event-sourcing-error
      (subscription-lease-acquire store "consumer" nil :now 0))
    (signals type-error
      (subscription-lease-acquire store "consumer" "owner" :now "now"))
    (signals type-error
      (subscription-lease-acquire store "consumer" "owner" :lease-seconds 0))
    (signals type-error
      (subscription-lease-acquire store "consumer" "owner"
                                  :lease-seconds "not-a-duration"))
    (signals type-error
      (subscription-lease-renew store 1 :now 0))
    (signals type-error
      (subscription-lease-valid-p store 1 :now 0))
    (signals type-error
      (subscription-current-lease 1))
    (signals type-error
      (subscription-renew-lease 1 :now 0))
    (signals type-error
      (subscription-release-lease 1)))))

(describe
 "leased subscriptions"
 (it
  "requires an owner and fences a stale subscriber before acknowledgement"
  (let* ((event-store (make-in-memory-event-store :global-position-start 0))
         (offset-store (make-in-memory-subscription-offset-store))
         (lease-store (make-in-memory-subscription-lease-store)))
    (signals type-error
      (make-subscription :event-store event-store
                         :offset-store offset-store
                         :consumer-id "invalid-lease-store"
                         :lease-store 1
                         :owner-id "owner"))
    (signals event-sourcing-error
      (make-subscription :event-store event-store
                         :offset-store offset-store
                         :consumer-id "missing-owner"
                         :lease-store lease-store))
    (signals type-error
      (make-subscription :event-store event-store
                         :offset-store offset-store
                         :consumer-id "invalid-lease-seconds"
                         :lease-store lease-store
                         :owner-id "owner"
                         :lease-seconds 0))
    (signals event-sourcing-error
      (make-subscription :event-store event-store
                         :offset-store offset-store
                         :consumer-id "owner-without-store"
                         :owner-id "owner"))
    (let ((ordinary
            (make-subscription :event-store event-store
                               :offset-store offset-store
                               :consumer-id "ordinary")))
      (expect (subscription-current-lease ordinary) :to-be nil)
      (expect (subscription-renew-lease ordinary :now 0) :to-be nil)
      (expect (subscription-release-lease ordinary) :to-be nil))
    (let ((applied-default
            (apply #'make-subscription
                   (list :event-store event-store
                         :offset-store offset-store
                         :consumer-id "applied-default"))))
      (expect (subscription-lease-seconds applied-default) :to-be 60))
    (event-store-append
     event-store
     "leased-stream"
     (list (make-test-event "leased-event" "leased-stream" :payload)))
    (let* ((owner-a
             (make-subscription :event-store event-store
                                :offset-store offset-store
                                :consumer-id "leased-consumer"
                                :lease-store lease-store
                                :owner-id "owner-a"
                                :lease-seconds 60))
           (owner-b
             (make-subscription :event-store event-store
                                :offset-store offset-store
                                :consumer-id "leased-consumer"
                                :lease-store lease-store
                                :owner-id "owner-b"
                                :lease-seconds 60))
           (lease-a (subscription-renew-lease owner-a)))
      (expect (subscription-owner-id owner-a) :to-equal "owner-a")
      (expect (subscription-lease-seconds owner-a) :to-be 60)
      (expect (subscription-current-lease owner-a) :to-be lease-a)
      (let ((delivery (subscription-poll owner-a)))
        (expect (subscription-delivery-p delivery) :to-be-truthy)
        (let* ((position
                 (subscription-delivery-after-global-position delivery))
               (lease-b
                 (subscription-lease-acquire
                  lease-store
                  "leased-consumer"
                  "owner-b"
                  :now (+ (subscription-lease-expires-at lease-a) 1)
                  :lease-seconds 60)))
          (expect (subscription-lease-fencing-token lease-b)
                  :to-be
                  (1+ (subscription-lease-fencing-token lease-a)))
          (signals event-sourcing-error
            (subscription-ack owner-a position))
          (let ((second-delivery (subscription-poll owner-b)))
            (expect (subscription-delivery-p second-delivery)
                    :to-be-truthy)
            (expect (subscription-ack
                     owner-b
                     (subscription-delivery-after-global-position
                      second-delivery))
                    :to-be
                    position))
          (expect (subscription-release-lease owner-a) :to-be nil)
          (expect (subscription-current-lease owner-a) :to-be nil)
          (expect (subscription-release-lease owner-b) :to-be-truthy)
          (expect (subscription-current-lease owner-b) :to-be nil)))))))
