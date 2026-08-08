(defun %invalid-domain-event (event reason &optional message)
  (error
   'invalid-domain-event
   :event
   event
   :reason
   reason
   :message
   (or message "The domain event is invalid.")))

(defun %signal-unsupported-event-store-operation (store operation)
  (error 'event-store-operation-not-supported :operation operation :store store))
