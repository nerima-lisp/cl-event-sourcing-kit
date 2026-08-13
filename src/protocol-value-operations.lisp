;;;; Package selection is a reader-time declaration.  Keeping it out of the
;;;; compiled form lets SB-COVER measure the operations below without
;;;; counting an uncallable load-time form as runtime behavior.
#.(progn (in-package #:cl-event-sourcing-kit) nil)

(defun event-snapshot-p (object)
  (typep object 'event-snapshot))

(defun make-event-snapshot (&key
                            (stream-id *unspecified*)
                            (version *unspecified*)
                            state
                            metadata
                            timestamp)
  "Create an opaque aggregate snapshot value.

The STATE is caller-owned and is not serialized or deep-copied by the core."
  (when (or (eq stream-id *unspecified*) (null stream-id))
    (error
     'invalid-snapshot
     :stream-id stream-id
     :version version
     :reason :stream-id))
  (unless (and (integerp version) (<= 0 version))
    (error
     'invalid-snapshot
     :stream-id stream-id
     :version version
     :reason :version))
  (make-instance
   'event-snapshot
   :stream-id stream-id
   :version version
   :state state
   :metadata metadata
   :timestamp timestamp))

(defun make-event-append-request (&key
                                  (stream-id *unspecified*)
                                  events
                                  (expected-version *unspecified*))
  "Create one request for EVENT-STORE-APPEND-BATCH.

The backend performs event-envelope and stream-order validation when the
request is committed."
  (let ((effective-expected-version
          (if (eq expected-version *unspecified*)
              :any
              expected-version)))
    (when (or (eq stream-id *unspecified*) (null stream-id))
      (%invalid-domain-event events :stream-id))
    (unless (%valid-expected-version-p effective-expected-version)
      (error 'invalid-expected-version :value effective-expected-version))
    (%make-event-append-request
     stream-id
     events
     effective-expected-version)))

(defun %valid-expected-version-p (expected-version)
  (or
   (eq expected-version :any)
   (eq expected-version :no-stream)
   (and (integerp expected-version) (<= 0 expected-version))))

(defun upcast-event (event upcaster)
  "Transform EVENT into the schema understood by the caller.

UPCASTER is NIL or a function receiving one DOMAIN-EVENT and returning a new
DOMAIN-EVENT.  A registry, chained upcaster, or backend-specific policy can
be supplied as that function without coupling the core to a serialization
format."
  (unless (domain-event-p event)
    (%invalid-domain-event event :event-type))
  (if (null upcaster)
      event
      (progn
        (unless (functionp upcaster)
          (error 'type-error :datum upcaster :expected-type 'function))
        (let ((upcasted-event (funcall upcaster event)))
          (unless (domain-event-p upcasted-event)
            (%invalid-domain-event upcasted-event :upcaster-result))
          upcasted-event))))

(defun upcast-events (events upcaster)
  "Upcast every event in EVENTS while preserving its order."
  (mapcar
   (lambda (event) (upcast-event event upcaster))
   (%coerce-event-list events)))
