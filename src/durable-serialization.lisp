;;;; Safe values and event envelopes

(defun %durable-lock (&optional name)
  (let ((key (or name "cl-event-sourcing-kit/durable")))
    (cl-concurrent-kit:with-lock-held (*durable-lock-registry-lock*)
      (or (gethash key *durable-lock-registry*)
          (setf (gethash key *durable-lock-registry*)
                (cl-concurrent-kit:make-lock :name key))))))

(defun %resolve-event-serializer (serializer)
  (cond
    ((null serializer) (make-event-serializer))
    ((event-serializer-p serializer) serializer)
    (t (error 'type-error :datum serializer :expected-type 'event-serializer))))

(defun %safe-symbol-package-p (symbol)
  (let ((package (symbol-package symbol)))
    (and package
         (member (package-name package)
                 '("COMMON-LISP" "COMMON-LISP-USER" "KEYWORD"
                   "CL-EVENT-SOURCING-KIT")
                 :test #'string=))))

(defun %safe-serializable-value-p (value
                                   &key
                                   (max-depth *unspecified* max-depth-supplied-p))
  "Return true when VALUE can be represented by the default reader safely.

The default codec intentionally accepts only portable atoms and vectors/cons
trees made from those atoms.  Domain values such as UUID objects, hash tables,
pathnames, CLOS instances, and foreign pointers require an application codec.
  Circular data is rejected because it is a common source of unbounded work at
  serialization boundaries."
  (unless max-depth-supplied-p
    (setf max-depth 64))
  (unless (and (integerp max-depth) (plusp max-depth))
    (error 'type-error :datum max-depth :expected-type '(integer 1 *)))
  (labels ((walk (value active depth)
             (cond
               ((or (null value) (characterp value)) t)
               ((and (numberp value) (realp value)) t)
               ((stringp value) t)
               ((symbolp value) (%safe-symbol-package-p value))
               ((consp value)
                (when (> depth max-depth)
                  (return-from walk nil))
                (when (gethash value active)
                  (return-from walk nil))
                (setf (gethash value active) t)
                (unwind-protect
                     (and (walk (car value) active (1+ depth))
                          (walk (cdr value) active (1+ depth)))
                  (remhash value active)))
               ((vectorp value)
                (when (> depth max-depth)
                  (return-from walk nil))
                (when (gethash value active)
                  (return-from walk nil))
                (setf (gethash value active) t)
                (unwind-protect
                     (loop for index below (length value)
                           always (walk (aref value index) active (1+ depth)))
                  (remhash value active)))
               (t nil))))
    (walk value (make-hash-table :test #'eq) 0)))

(defun %default-serialize-value (value serializer)
  (unless (%safe-serializable-value-p
           value
           :max-depth (%event-serializer-max-depth serializer))
    (error "The default serializer only accepts portable acyclic values."))
  (labels ((portable-copy (value)
             (cond
               ;; SBCL prints SIMPLE-BASE-STRING values as #A dispatch
               ;; syntax.  Copy strings to a general character vector so
               ;; the default wire format remains reader-portable.
               ((stringp value)
                (let ((copy (make-array (length value)
                                        :element-type 'character)))
                  (replace copy value)
                  copy))
               ((consp value)
                (cons (portable-copy (car value))
                      (portable-copy (cdr value))))
               ((vectorp value)
                (let ((copy (make-array (length value) :element-type t)))
                  (loop for index below (length value)
                        do (setf (aref copy index)
                                 (portable-copy (aref value index))))
                  copy))
               (t value))))
  (with-standard-io-syntax
    (let ((*print-readably* t)
          (*print-circle* nil)
          (*print-pretty* nil))
      (write-to-string (portable-copy value) :readably t :circle nil)))))

(defun %unsafe-reader-dispatch-p (text)
  (loop for index below (length text)
        thereis (and (char= (char text index) #\#)
                     (< (1+ index) (length text))
                     (let ((next (char text (1+ index))))
                       (or (find next ".,'" :test #'char=)
                           (find next "sSpPcCaA" :test #'char=))))))

(defun %utf-8-octet-length (text)
  (loop for character across text
        for code = (char-code character)
        sum (cond
              ((<= code #x7f) 1)
              ((<= code #x7ff) 2)
              ((<= code #xffff) 3)
              (t 4))))

(defun %default-deserialize-value (text serializer)
  (unless (stringp text)
    (error 'type-error :datum text :expected-type 'string))
  (when (%unsafe-reader-dispatch-p text)
    (error "The default deserializer rejected an unsafe reader dispatch."))
  (let ((eof (gensym "EOF")))
    (handler-case
        (let ((value
                (with-input-from-string (stream text)
                  (with-standard-io-syntax
                    (let ((*read-eval* nil))
                      (let ((first (read stream nil eof)))
                        (unless (eq (read stream nil eof) eof)
                          (error "Serialized input contains more than one form."))
                        first))))))
          (when (eq value eof)
            (error "Serialized input is empty."))
          (unless (%safe-serializable-value-p
                   value
                   :max-depth (%event-serializer-max-depth serializer))
            (error "The default deserializer produced an unsupported value."))
          value)
      (error (cause)
        (error 'event-serialization-error
               :value text
               :direction :decode
               :cause cause)))))

(defun serialize-value (value &key serializer)
  "Encode VALUE to a string using SERIALIZER or the safe default codec."
  (let* ((serializer (%resolve-event-serializer serializer))
         (encoder (%event-serializer-encode serializer)))
    (handler-case
        (let ((encoded (if encoder
                          (funcall encoder value)
                          (%default-serialize-value value serializer))))
          (unless (stringp encoded)
            (error 'type-error :datum encoded :expected-type 'string))
          (when (> (%utf-8-octet-length encoded)
                   (%event-serializer-max-bytes serializer))
            (error "Serialized value exceeds the configured byte limit."))
          encoded)
      (event-serialization-error (condition) (error condition))
      (error (cause)
        (error 'event-serialization-error
               :value value
               :direction :encode
               :cause cause)))))

(defun deserialize-value (serialized &key serializer)
  "Decode a serialized value using SERIALIZER or the safe default codec."
  (let* ((serializer (%resolve-event-serializer serializer))
         (decoder (%event-serializer-decode serializer)))
    (handler-case
        (progn
          (unless (stringp serialized)
            (error 'type-error :datum serialized :expected-type 'string))
          (when (> (%utf-8-octet-length serialized)
                   (%event-serializer-max-bytes serializer))
            (error "Serialized value exceeds the configured byte limit."))
          (if decoder
              (funcall decoder serialized)
              (%default-deserialize-value serialized serializer)))
      (event-serialization-error (condition) (error condition))
      (error (cause)
        (error 'event-serialization-error
               :value serialized
               :direction :decode
               :cause cause)))))

(defun %required-wire-value (wire key)
  (let* ((missing (gensym "MISSING"))
         ;; Durable records use a keyword discriminator followed by a
         ;; property list.  Strip the discriminator before looking up a
         ;; property, while still accepting an ordinary property list.
         (properties
           (if (and (%proper-list-p wire)
                    (consp wire)
                    (keywordp (first wire))
                    (evenp (length (rest wire))))
               (rest wire)
             wire)))
    (let ((value (getf properties key missing)))
      (when (eq value missing)
        (error "Serialized record is missing ~S." key))
      value)))

(defun %domain-event-wire (event)
  (unless (domain-event-p event)
    (%invalid-domain-event event :event-type))
  (list :domain-event
        :id (domain-event-id event)
        :type (domain-event-type event)
        :stream-id (domain-event-stream-id event)
        :aggregate-id (domain-event-aggregate-id event)
        :payload (domain-event-payload event)
        :metadata (domain-event-metadata event)
        :timestamp (domain-event-timestamp event)
        :schema-version (domain-event-schema-version event)
        :version (domain-event-version event)
        :correlation-id (domain-event-correlation-id event)
        :causation-id (domain-event-causation-id event)
        :global-position (domain-event-global-position event)))

(defun %domain-event-from-wire (wire)
  (unless (and (%proper-list-p wire) (eq (first wire) :domain-event))
    (error "Serialized value is not a domain-event record."))
  (handler-case
      (make-domain-event
       :id (%required-wire-value wire :id)
       :type (%required-wire-value wire :type)
       :stream-id (%required-wire-value wire :stream-id)
       :aggregate-id (%required-wire-value wire :aggregate-id)
       :payload (%required-wire-value wire :payload)
       :metadata (%required-wire-value wire :metadata)
       :timestamp (%required-wire-value wire :timestamp)
       :schema-version (%required-wire-value wire :schema-version)
       :version (%required-wire-value wire :version)
       :correlation-id (%required-wire-value wire :correlation-id)
       :causation-id (%required-wire-value wire :causation-id)
       :global-position (%required-wire-value wire :global-position))
    (event-sourcing-error (condition) (error condition))
    (error (cause)
      (error 'event-serialization-error
             :value wire
             :direction :decode
             :cause cause))))

(defun serialize-domain-event (event &key serializer)
  (serialize-value (%domain-event-wire event) :serializer serializer))

(defun deserialize-domain-event (serialized &key serializer)
  (%domain-event-from-wire
   (deserialize-value serialized :serializer serializer)))

(defun %snapshot-wire (snapshot)
  (%validate-event-snapshot snapshot)
  (list :event-snapshot
        :stream-id (event-snapshot-stream-id snapshot)
        :version (event-snapshot-version snapshot)
        :state (event-snapshot-state snapshot)
        :metadata (event-snapshot-metadata snapshot)
        :timestamp (event-snapshot-timestamp snapshot)))

(defun %snapshot-from-wire (wire)
  (unless (and (%proper-list-p wire) (eq (first wire) :event-snapshot))
    (error "Serialized value is not an event-snapshot record."))
  (make-event-snapshot
   :stream-id (%required-wire-value wire :stream-id)
   :version (%required-wire-value wire :version)
   :state (%required-wire-value wire :state)
   :metadata (%required-wire-value wire :metadata)
   :timestamp (%required-wire-value wire :timestamp)))

;;;; Atomic auxiliary files and the append log

(defun %durable-pathname (path)
  (unless (or (stringp path) (pathnamep path))
    (error 'type-error :datum path :expected-type '(or string pathname)))
  (let ((pathname (pathname path)))
    (ensure-directories-exist pathname)
    pathname))

(defun %durable-temporary-pathname (path)
  (make-pathname
   :name (format nil ".cl-event-sourcing-kit-~A" (gensym "tmp"))
   ;; RENAME-FILE may inherit the source type when the destination type is
   ;; NIL.  Keep the temporary pathname's type aligned with PATH so an
   ;; extensionless destination is replaced at the requested pathname.
   :type (pathname-type path)
   :defaults path))

(defun %write-serialized-file (path value serializer
                               &optional (sync #'finish-output))
  (let* ((path (%durable-pathname path))
         (temporary (%durable-temporary-pathname path))
         (serialized (serialize-value value :serializer serializer)))
    (unless (functionp sync)
      (error 'type-error :datum sync :expected-type 'function))
    (when (or (position #\Newline serialized)
              (position #\Return serialized))
      (error 'event-serialization-error
             :value value
             :direction :encode
             :cause "Auxiliary durable records must be one line."))
    (unwind-protect
         (progn
           (with-open-file (stream temporary
                                   :direction :output
                                   :if-exists :supersede
                                   :if-does-not-exist :create)
             (write-line serialized stream)
             (funcall sync stream))
           ;; RENAME-FILE is the only replacement step.  If the host cannot
           ;; replace PATH atomically, retain the old value and let the
           ;; caller observe the failure; deleting it first would turn a
           ;; recoverable write error into data loss.
           (rename-file temporary path)
           value)
      (when (probe-file temporary)
        (delete-file temporary)))))

(defun %read-file-string (path)
  (with-open-file (stream path :direction :input)
    (let ((output (make-string-output-stream)))
      (loop for character = (read-char stream nil nil)
            while character
            do (write-char character output))
      (get-output-stream-string output))))

(defun %read-serialized-file (path serializer)
  (let ((path (pathname path)))
    (if (probe-file path)
        (deserialize-value (%read-file-string path) :serializer serializer)
        nil)))
