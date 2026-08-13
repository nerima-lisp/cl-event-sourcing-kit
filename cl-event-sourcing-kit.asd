(in-package #:asdf-user)

(asdf:defsystem "cl-event-sourcing-kit"
  :description "A domain-independent event sourcing protocol for Common Lisp."
  :long-description "The core defines an opaque event envelope, optimistic append/read protocol,
pure replay, staging, structured conditions, and synchronous CPS entry points.
Persistence, serialization, durability, scheduling, and domain policy belong to
the concrete store and optional systems."
  :author "nerima-lisp"
  :maintainer "nerima-lisp"
  :homepage "https://github.com/nerima-lisp/cl-event-sourcing-kit"
  :bug-tracker "https://github.com/nerima-lisp/cl-event-sourcing-kit/issues"
  :source-control (:git
                   "https://github.com/nerima-lisp/cl-event-sourcing-kit.git")
  :license "MIT"
  :version "1.0.0"
  :depends-on ("cl-boundary-kit" "cl-weave")
  :pathname "src"
  :serial t
  :around-compile (lambda (next)
                    (let ((*package*
                           (or (find-package "CL-EVENT-SOURCING-KIT") *package*)))
                      (funcall next)))
  :components ((:file "package")
               (:file "conditions")
               (:file "condition-operations")
               (:file "event-model")
               (:file "event-operations")
               (:file "protocol-model")
               (:file "protocol-value-operations")
               (:file "protocol-operations")
               (:file "protocol-defaults")
               (:file "replay")
               (:file "staging-model")
               (:file "staging")
               (:file "macro-dsl")
               (:file "cps-macros")
               (:file "cps"))
  :in-order-to ((test-op (test-op "cl-event-sourcing-kit/test"))))

(asdf:defsystem "cl-event-sourcing-kit/in-memory"
  :description "The thread-safe in-memory reference event store."
  :depends-on ("cl-event-sourcing-kit" "cl-concurrent-kit")
  :pathname "src"
  :serial t
  :around-compile (lambda (next)
                    (let ((*package*
                           (or (find-package "CL-EVENT-SOURCING-KIT") *package*)))
                      (funcall next)))
  :components ((:file "in-memory-model")
               (:file "in-memory-operations")
               (:file "in-memory-snapshots")
               (:file "in-memory-queries")))

(asdf:defsystem "cl-event-sourcing-kit/projection"
  :description "Optional projection state and rebuild support."
  :depends-on ("cl-event-sourcing-kit")
  :pathname "src"
  :serial t
  :around-compile (lambda (next)
                    (let ((*package*
                           (or (find-package "CL-EVENT-SOURCING-KIT") *package*)))
                      (funcall next)))
  :components ((:file "projection-model")
               (:file "projection-operations")
               (:file "projection-rebuild")
               (:file "projection-macros")))

(asdf:defsystem "cl-event-sourcing-kit/durable"
  :description "Durable reference implementation and application-runtime protocols."
  :long-description "A safe S-expression serializer, crash-tolerant append log
implementation, at-least-once subscriptions with offsets, application outbox,
durable projection checkpoints, upcaster registry, and operational wrappers."
  :depends-on ("cl-event-sourcing-kit/in-memory"
               "cl-event-sourcing-kit/projection"
               "cl-concurrent-kit")
  :pathname "src"
  :serial t
  :around-compile (lambda (next)
                    (let ((*package*
                           (or (find-package "CL-EVENT-SOURCING-KIT") *package*)))
                      (funcall next)))
  :components ((:file "durable-model")
               (:file "durable-model-operations")
               (:file "durable-macros")
               (:file "durable-serialization")
               (:file "durable-file-store")
               (:file "durable-file-operations")
               (:file "durable-subscription-lease")
               (:file "durable-subscription")
               (:file "durable-outbox-storage")
               (:file "durable-outbox-processing")
               (:file "durable-outbox-lifecycle")
               (:file "durable-outbox-integration")
               (:file "durable-projection")
               (:file "durable-evolution")))

(asdf:defsystem "cl-event-sourcing-kit/test"
  :description "Public API tests for cl-event-sourcing-kit."
  :depends-on ("cl-event-sourcing-kit/durable"
               "cl-host-kit"
               "cl-weave")
  :pathname "t"
  :serial t
  :components ((:file "package")
               (:file "support")
               (:file "edge-contract-test")
               (:file "event-test")
               (:file "store-test")
               (:file "replay-staging-test")
               (:file "projection-test")
               (:file "macro-cps-test")
               (:file "quick-start-test")
               (:file "durable-test")
               (:file "durable-contract-test")
               (:file "durable-edge-test")
               (:file "durable-boundary-test")
               (:file "durable-lease-test")
               (:file "capability-test"))
  :perform (test-op
            (operation component)
            (declare (ignore operation component))
            (unless (funcall
                     (symbol-function
                      (find-symbol
                       "RUN-TESTS"
                       "CL-EVENT-SOURCING-KIT/TEST")))
              (error "cl-event-sourcing-kit tests failed."))))
