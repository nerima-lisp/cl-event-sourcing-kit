;;;; run-tests.lisp
;;;;
;;;; Bootstrap the checkout into ASDF's source registry and run the public
;;;; test system.  Dependencies are supplied by the caller's ASDF setup or by
;;;; cl-nix-forge; this script does not mutate a user's global configuration.

(require :asdf)

(asdf:load-system "cl-host-kit")

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
  (asdf:test-system "cl-event-sourcing-kit")
  (host-kit:quit 0))
