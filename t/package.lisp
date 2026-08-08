(defpackage #:cl-event-sourcing-kit/test (:use #:cl #:cl-event-sourcing-kit)
  (:import-from #:asdf #:system-relative-pathname)
  (:shadowing-import-from #:cl-weave #:describe)
  (:import-from #:cl-weave #:expect #:it #:run-all #:signals)
  (:export #:run-tests))

(in-package #:cl-event-sourcing-kit/test)

(defun runtime-source-paths ()
  (mapcar
   (lambda (name)
     (truename
      (system-relative-pathname
       "cl-event-sourcing-kit"
       (format nil "src/~A.lisp" name))))
   '("condition-operations"
     "cps"
     "event-operations"
     "in-memory-operations"
     "projection-operations"
     "projection-rebuild"
     "protocol-defaults"
     "protocol-operations"
     "replay"
     "staging")))

(defun run-tests (&key coverage)
  (run-all
   :reporter
   :spec
   :timeout-ms
   120000
   :pass-with-no-tests
   nil
   :coverage
   coverage
   :coverage-report-directory
   (host-kit:ensure-directory-pathname
    (or
     (host-kit:getenv "CL_EVENT_SOURCING_KIT_COVERAGE_DIR")
     (merge-pathnames
      "cl-event-sourcing-kit-coverage/"
      (host-kit:temporary-directory))))
   :coverage-include-pathnames
   (runtime-source-paths)
   :coverage-minimum-expression
   100
   :coverage-minimum-branch
   100))
