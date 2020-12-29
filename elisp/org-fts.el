;;; org-fts.el --- Full text search for org files with ivy support -*- lexical-binding: t; -*-

;; (c) 2020 by Bill Burdick, shared under MIT license

;; Author: Bill Burdick <bill.burdick@gmail.com>
;; URL: https://github.com/zot/microfts/tree/main/elisp/org-fts.el
;; Version: 1.0.0
;; Package-Requires: ((org "9.1.0") (emacs "25.1"))
;; Keywords: org convenience

;;; Commentary:

;; Org-fts automatically indexes changed org files as you visit them
;; and save them. If you delete or rename an org file, org-fts will
;; lose track of it until you visit it again.

;; Searching is based on microfts, which is fairly fast, (returning
;; results in less than 10ms for around 3M of files). Full text
;; indexing takes space, so expect the index to take maybe 2-5 times
;; the space of your files. Each keystroke in the screencast below
;; does another full text search with microfts.

;; Thanks to Professor John Kitchin for testing and contributions

;;; Code:

(require 'cl-lib)
(require 'org)
(require 'package)
(require 'executable)

(defgroup org-fts ()
  "Customization for org-fts"
  :group 'org)

(defconst org-fts-microfts-url-alist
  '((gnu/linux . "https://github.com/zot/microfts/releases/download/v1.0.0/microfts-linux.gz")
    (windows-nt . "https://github.com/zot/microfts/releases/download/v1.0.0/microfts-windows.gz")
    (darwin . "https://github.com/zot/microfts/releases/download/v1.0.0/microfts-mac.gz")))

(defconst org-fts-baseprogram-alist
  '((gnu/linux . "microfts")
    (darwin . "microfts")
    (windows-nt . "microfts.exe")))

(defconst org-fts-baseprogram
  (let ((name (cdr (assoc system-type org-fts-baseprogram-alist))))
    (format "%s%s" user-emacs-directory name)))

(defcustom org-fts-program nil
  "Name or path for microfts program."
  :type 'string
  :group 'org-fts)

(defcustom org-fts-db (file-truename (format "%sorg-fts.db" user-emacs-directory))
  "Path to the org-fts database file."
  :type 'string
  :group 'org-fts)

(defcustom org-fts-search-args '("-partial")
  "Default arguments to search."
  :type '(repeat string)
  :group 'org-fts)

(defcustom org-fts-input-args '("-org")
  "Default arguments to input."
  :type '(repeat string)
  :group 'org-fts)

(defvar org-fts-timer (run-with-idle-timer (* 60 5) t 'org-fts-idle-task))
(defvar org-fts-actual-program nil)

(defun org-fts-check-db ()
  "Create fts db if it is not there."
  (and (org-fts-test org-fts-db "need to customize org-fts-db, it has no value")
       (org-fts-ensure-binary)
       (or (file-exists-p org-fts-db)
           (let ((ret (call-process org-fts-actual-program nil nil nil
                                    "create" org-fts-db)))
             (org-fts-test (eql 0 ret) "creating database %s returned error %s" org-fts-db ret)))))

(defun org-fts-test (test &rest format)
  "If TEST fails, use message to print FORMAT args."
  (or test
      (progn
        (apply 'message format)
        nil)))

(defun org-fts-update-hook ()
  "A file was just opened or saved, index/reindex it if it's an `org-mode' file."
  (when (and
         (buffer-file-name)
	     (equal major-mode 'org-mode)
         (org-fts-check-db))
    (apply #'start-process "org-fts" nil org-fts-actual-program
           `("input" ,@org-fts-input-args ,org-fts-db ,(file-truename (buffer-file-name))))))

(add-hook 'org-mode-hook 'org-fts-update-hook)
(add-hook 'after-save-hook 'org-fts-update-hook)

(defun org-fts-idle-task ()
  "After a certain amount of idle time, update and compact the org-fts database."
  (when (org-fts-check-db)
    (let ((process (start-process "org-fts" nil org-fts-actual-program
                                  "update" org-fts-db)))
      (set-process-sentinel process
                            (lambda (process status)
                              (ignore process)
                              (when (equal status "finished\n")
                                (start-process "org-fts" nil org-fts-actual-program
                                               "compact" org-fts-db)))))))

(defun org-fts-microfts-basic-search (terms)
  "Call microfts with TERMS and return results."
  (when (and terms (org-fts-check-db))
    (with-current-buffer (get-buffer-create "org-search")
      (erase-buffer)
      (let ((ret (apply #'call-process org-fts-actual-program nil "org-search" nil
                        `("search" "-sexp" ,@org-fts-search-args ,org-fts-db ,@terms))))
        (if (not (eql ret 0))
            (progn
              (message "Search returned error %s" ret)
              nil)
          (mapcar 'read (split-string (buffer-string) "\n" t)))))))

(defun org-fts-split-terms (term-str)
  "Split TERM-STR into parts, respecting quotes."
  (condition-case nil (split-string-and-unquote term-str) ((debug error) nil)))

(defun org-fts-microfts-search (term-str)
  "Non-fancy search for TERM-STR."
  (mapcar (lambda (line)
            (let ((file (plist-get line :filename))
                  (text (replace-regexp-in-string "\n" "\\\\n" (plist-get line :text))))
              (format "%s:%s: %s" file (plist-get line :line)  text)))
          (org-fts-microfts-basic-search (org-fts-split-terms term-str))))

(defun org-fts-ensure-binary ()
  "Make sure microfts is present."
  (setq org-fts-actual-program
        (or
         (and org-fts-program
              (or (file-exists-p org-fts-program)
                  (error "Program not found: %s" org-fts-program))
              (or (file-executable-p org-fts-program)
                  (error "Program exists but is not executable: %s" org-fts-program))
              org-fts-program)
         (and (file-exists-p org-fts-baseprogram)
              (or (file-executable-p org-fts-baseprogram)
                  (error "Program exists but is not executable: %s" org-fts-baseprogram))
              org-fts-baseprogram)
         ;; if baseprogram is not there, download it
         (let* ((url (cdr (assoc system-type org-fts-microfts-url-alist)))
                (comprfile (format "%s.gz" org-fts-baseprogram))
                (compr auto-compression-mode))
           (when (not org-fts-baseprogram) (error "No value for org-fts-baseprogram or org-fts-program"))
           (when (not url) (error "Unsupported system type: %s" system-type))
           (url-copy-file url comprfile t)
           (save-excursion
             (unwind-protect
                 (progn
                   (if compr (auto-compression-mode -1))
                   (find-file-literally comprfile)
                   (zlib-decompress-region (point-min) (point-max))
                   (write-file comprfile)
                   (executable-chmod)
                   (rename-file comprfile org-fts-baseprogram)
                   (kill-buffer)
                   org-fts-baseprogram)
               (if compr (auto-compression-mode 1))))))))

(defun org-fts-search (terms)
  "Search org files for TERMS."
  (interactive "sOrg search: ")
  (minibuffer-with-setup-hook (lambda ())
    (completing-read "Org file results: " (org-fts-microfts-search terms))))

(defun org-fts-find-org-file ()
  "Find one of your org files."
  (interactive)
  (when (org-fts-check-db)
    (with-current-buffer (get-buffer-create "org-fts")
      (erase-buffer)
      (call-process org-fts-actual-program nil "org-fts" nil
                    "info" "-groups" org-fts-db)
      (goto-char (point-min))
      (perform-replace " *\\(org-mode\\)?\\( DELETED\\| NOT AVAILABLE\\| CHANGED\\)?$" "" nil t nil)
      (let ((files (seq-filter (lambda (x) (> (length x) 0)) (split-string (buffer-string) "\n"))))
        (minibuffer-with-setup-hook (lambda ())
          (completing-read "Org files: " (seq-sort (lambda (a b)
                                                (string-collate-lessp
                                                 (downcase a) (downcase b))) files)))))))

(provide 'org-fts)

;;; org-fts.el ends here
