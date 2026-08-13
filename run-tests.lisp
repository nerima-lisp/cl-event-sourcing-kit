;;;; run-tests.lisp
;;;;
;;;; Bootstrap the checkout into ASDF's source registry and run the public
;;;; test system.  Dependencies are supplied by the caller's ASDF setup or by
;;;; cl-nix-forge; this script does not mutate a user's global configuration.

(require :asdf)

(defun script-directory ()
  (make-pathname :name nil
                 :type nil
                 :defaults (or *load-truename*
                               *compile-file-truename*
                               (error "Unable to determine the script location"))))

(defun configure-local-source-registry (root)
  (asdf:initialize-source-registry
   `(:source-registry
     (:tree ,root)
     :inherit-configuration)))

(let ((root (script-directory)))
  (configure-local-source-registry root)
  (format t "~&[cl-event-sourcing-kit] loading cl-host-kit...~%")
  (finish-output)
  (asdf:load-system "cl-host-kit")
  (format t "~&[cl-event-sourcing-kit] loading test system from ~A...~%" root)
  (finish-output)
  (asdf:test-system "cl-event-sourcing-kit")
  (funcall (symbol-function (find-symbol "QUIT" "HOST-KIT")) 0))
