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

(defun test-source-paths ()
  (let ((symbol
          (find-symbol
           "RUNTIME-SOURCE-PATHS"
           "CL-EVENT-SOURCING-KIT/TEST")))
    (unless (and symbol (fboundp symbol))
      (error "The test system did not expose its source selector."))
    (let ((paths (funcall (symbol-function symbol))))
      (unless paths
        (error "The coverage source selector returned no files."))
      paths)))

(defun full-coverage-p (statistics)
  (let ((expression-total (getf statistics :expression-total))
        (expression-covered (getf statistics :expression-covered))
        (branch-total (getf statistics :branch-total))
        (branch-covered (getf statistics :branch-covered)))
    (and (plusp expression-total)
         (= expression-covered expression-total)
         (or (zerop branch-total)
             (= branch-covered branch-total)))))

(defun coverage-statistics-for (source-paths)
  (let ((symbol (find-symbol "COVERAGE-STATISTICS" "CL-WEAVE")))
    (unless (and symbol (fboundp symbol))
      (error "cl-weave did not expose COVERAGE-STATISTICS."))
    (funcall (symbol-function symbol)
             :include-pathnames
             source-paths)))

(defun coverage-file-statistics-for (source-paths)
  (let* ((package (find-package "SB-COVER"))
         (refresh (and package (find-symbol "REFRESH-COVERAGE-BITS" package)))
         (coverage-info (and package
                             (find-symbol "*CODE-COVERAGE-INFO*" package)))
         (compute (and package (find-symbol "COMPUTE-FILE-INFO" package)))
         (ok-of (and package (find-symbol "OK-OF" package)))
         (all-of (and package (find-symbol "ALL-OF" package)))
         (names (mapcar (lambda (path)
                          (namestring (truename path)))
                        source-paths)))
    (unless (and refresh coverage-info compute ok-of all-of
                 (fboundp refresh)
                 (boundp coverage-info)
                 (fboundp compute)
                 (fboundp ok-of)
                 (fboundp all-of))
      (error "SB-COVER does not expose the per-file statistics API."))
    (funcall refresh)
    (let ((value (symbol-value coverage-info)))
      (unless (and (consp value) (hash-table-p (car value)))
        (error "SB-COVER coverage data has an unsupported representation."))
      (let ((rows nil))
        (maphash
         (lambda (source ignored)
           (declare (ignore ignored))
           (let ((source-name (namestring (pathname source))))
             (when (member source-name names :test #'string=)
               (let ((counts (funcall compute source :default)))
                 (push
                  (list :path source-name
                        :expression-covered
                        (funcall ok-of (getf counts :expression))
                        :expression-total
                        (funcall all-of (getf counts :expression))
                        :branch-covered
                        (funcall ok-of (getf counts :branch))
                        :branch-total
                        (funcall all-of (getf counts :branch)))
                  rows)))))
         (car value))
        (sort rows #'>
              :key (lambda (row)
                     (+ (- (getf row :expression-total)
                           (getf row :expression-covered))
                        (- (getf row :branch-total)
                           (getf row :branch-covered)))))))))

(defun run-coverage-in-directory (fasl-root)
  ;; Nix makes dependency source trees read-only.  The intentional :force :all
  ;; load below must therefore send every FASL, including dependency FASLs, to
  ;; this writable directory.
  (asdf:initialize-output-translations
   `(:output-translations
     (t ,fasl-root)
     :ignore-inherited-configuration))
  (let ((passed-p nil)
        (coverage-condition nil)
        (statistics nil)
        (source-paths nil))
    (sb-cover:enable-coverage-logging)
    (let ((asdf:*compile-file-warnings-behaviour* :warn)
          (asdf:*compile-file-failure-behaviour* :error))
      (setf passed-p
            (handler-case
                (progn
                  (asdf:load-system
                   "cl-event-sourcing-kit/test"
                   :force
                   :all)
                  (setf source-paths (test-source-paths))
                  (funcall
                   (symbol-function
                    (find-symbol
                     "RUN-TESTS"
                     "CL-EVENT-SOURCING-KIT/TEST"))
                   :coverage
                   t)
                  t)
              (error (condition)
                (setf coverage-condition condition)
                (format *error-output* "~&Coverage run failed: ~A~%"
                        condition)
                nil)))
      (when source-paths
        (setf statistics
              (handler-case
                  (coverage-statistics-for source-paths)
                (error (condition)
                  (format *error-output*
                          "~&Coverage statistics failed: ~A~%"
                          condition)
                  nil))))
      (when statistics
        (format t "~&Coverage source files: ~D~%"
                (length source-paths))
        (format t "~&Coverage statistics: ~S~%" statistics)
        (dolist (row (coverage-file-statistics-for source-paths))
          (unless (and (= (getf row :expression-covered)
                          (getf row :expression-total))
                       (or (zerop (getf row :branch-total))
                           (= (getf row :branch-covered)
                              (getf row :branch-total))))
            (format t "~&Coverage gap: ~S~%" row))))
      (if (and passed-p statistics (full-coverage-p statistics))
          0
          (progn
            (unless coverage-condition
              (format *error-output*
                      "~&Coverage did not satisfy the complete-coverage gate.~%"))
            1)))))

(let* ((script
        (or *load-truename*
            (error "*LOAD-TRUENAME* is NIL; run this file as a script")))
       (root
        (make-pathname :name nil
                       :type nil
                       :version nil
                       :defaults (truename script))))
  (asdf:initialize-source-registry
   `(:source-registry (:directory ,root) :inherit-configuration))
  (declaim (optimize sb-cover:store-coverage-data))
  (host-kit:quit
   (host-kit:call-with-temporary-directory
    #'run-coverage-in-directory)))
