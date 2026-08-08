;;;; Compile the complete project under SBCL coverage instrumentation and run
;;;; the public test suite through cl-weave.
;;;;
;;;; Usage: sbcl --script run-coverage.lisp
;;;;
;;;; The ASDF :force :all load is intentional. :force t only recompiles the
;;;; test system, which can leave the project sources in cached, uninstrumented
;;;; fasls and make the coverage result vacuous.
(require :asdf)

(asdf:load-system "cl-host-kit")

(require :sb-cover)

(let* ((script
        (or
         *load-truename*
         (error "*LOAD-TRUENAME* is NIL; run this file as a script")))
       (root
        (make-pathname
         :name
         nil
         :type
         nil
         :version
         nil
         :defaults
         (truename script))))
  (asdf:initialize-source-registry
   `(:source-registry (:directory ,root) :inherit-configuration))
  (declaim (optimize sb-cover:store-coverage-data))
  (let ((exit-code
          (host-kit:call-with-temporary-directory
           (lambda (fasl-root)
             ;; Nix makes dependency source trees read-only.  The
             ;; intentional :force :all load below must therefore send every
             ;; FASL, including dependency FASLs, to this writable directory.
             (asdf:initialize-output-translations
              `(:output-translations
                (t ,fasl-root)
                :ignore-inherited-configuration))
             (let ((passed-p nil))
               (sb-cover:enable-coverage-logging)
               (let ((asdf:*compile-file-warnings-behaviour* :warn)
                     (asdf:*compile-file-failure-behaviour* :error))
                 (setf passed-p (handler-case (progn
                                                (asdf:load-system
                                                 "cl-event-sourcing-kit/test"
                                                 :force
                                                 :all)
                                                (funcall
                                                 (symbol-function
                                                  (find-symbol
                                                   "RUN-TESTS"
                                                   "CL-EVENT-SOURCING-KIT/TEST"))
                                                 :coverage
                                                 t))
                                    (error (condition)
                                      (format
                                       *error-output*
                                       "~&Coverage run failed: ~A~%"
                                       condition)
                                      nil)))
                 (if passed-p 0
                   1)))))))
    (host-kit:quit exit-code)))
