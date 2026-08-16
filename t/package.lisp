(defpackage #:cl-event-sourcing-kit/test (:use #:cl #:cl-event-sourcing-kit)
  (:import-from #:asdf #:find-system #:system-source-directory)
  (:shadowing-import-from #:cl-weave #:describe)
  (:import-from #:cl-weave
                #:expect
                #:gen-integer
                #:gen-list
                #:it
                #:it-each
                #:it-property
                #:run-all
                #:signals
                #:with-continuation-result)
  (:export #:run-tests))

(in-package #:cl-event-sourcing-kit/test)

(defparameter *declarative-source-file-names*
  '("package.lisp"
    "event-model.lisp"
    "protocol-model.lisp"
    "durable-model.lisp"
    "in-memory-model.lisp"
    "projection-model.lisp"
    "staging-model.lisp"
    "macro-dsl.lisp"
    "cps-macros.lisp"
    "durable-macros.lisp"
    "projection-macros.lisp")
  "Source files whose declarations or macro templates are checked structurally.

Executable functions remain in the coverage set.  Macro expansions are
covered through the operations that use them; the source files containing
only declarations or templates are accounted for here and checked with the
paredit definition inspector in the verification workflow.")

(defun all-source-paths ()
  (let ((paths
          (sort
           (directory
            (merge-pathnames
             "src/*.lisp"
             (system-source-directory
              (find-system "cl-event-sourcing-kit"))))
           #'string<
           :key #'namestring)))
    (unless paths
      (error "The core source path selector returned no files."))
    paths))

(defun source-file-name (path)
  (string-downcase (file-namestring path)))

(defun partition-source-paths ()
  (let* ((paths (all-source-paths))
         (available-names (mapcar #'source-file-name paths))
         (missing-declarations
           (set-difference *declarative-source-file-names*
                           available-names
                           :test #'string=)))
    (when missing-declarations
      (error
       "The declarative source manifest names missing files: ~S"
       missing-declarations))
    (values
     (remove-if
      (lambda (path)
        (member (source-file-name path)
                *declarative-source-file-names*
                :test #'string=))
      paths)
     (remove-if-not
      (lambda (path)
        (member (source-file-name path)
                *declarative-source-file-names*
                :test #'string=))
      paths))))

(defun runtime-source-paths ()
  (multiple-value-bind (runtime-paths declarative-paths)
      (partition-source-paths)
    (declare (ignore declarative-paths))
    (unless runtime-paths
      (error "The executable core source path selector returned no files."))
    runtime-paths))

(defun declarative-source-paths ()
  (multiple-value-bind (runtime-paths declarative-paths)
      (partition-source-paths)
    (declare (ignore runtime-paths))
    (unless declarative-paths
      (error "The declarative core source path selector returned no files."))
    declarative-paths))

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
