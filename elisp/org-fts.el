;;; org-fts.el --- Full text search for org files with ivy support -*- lexical-binding: t; -*-

;; (c) 2020 by Bill Burdick, shared under MIT license

;; Author: Bill Burdick <bill.burdick@gmail.com>
;; URL: https://github.com/zot/microfts/tree/main/elisp
;; Version: 1.0.0
;; Package-Requires: (cl-lib org ivy package executable)
;; Keywords: org convenience

;;; Commentary:

;;; Code:

(require 'cl-lib)
(require 'org)
(require 'ivy)
(require 'package)
(require 'executable)

(defgroup org-fts ()
  "Customization for org-fts"
  :group 'org)

(defconst org-fts/microfts-url-alist 
  '((gnu/linux . "https://github.com/zot/microfts/releases/download/v1.0.0/microfts-linux.gz")
    (windows-nt . "https://github.com/zot/microfts/releases/download/v1.0.0/microfts-windows.gz")
    (darwin . "https://github.com/zot/microfts/releases/download/v1.0.0/microfts-mac.gz")))

(defconst org-fts/baseprogram-alist
  '((gnu/linux . "microfts")
    (darwin . "microfts")
    (windows-nt . "microfts.exe")))

(defconst org-fts/baseprogram
  (let ((entry (cadr (assoc 'org-fts package-alist)))
        (name (cdr (assoc system-type org-fts/baseprogram-alist))))
    (and entry (format "%s/%s" (package-desc-dir entry) name))))

(defcustom org-fts/program nil
  "Name or path for microfts program"
  :type 'string
  :group 'org-fts)

(defcustom org-fts/db (file-truename (format "%sorg-fts.db" user-emacs-directory))
  "Path to the org-fts database file"
  :type 'string
  :group 'org-fts)

(defcustom org-fts/search-args '("-partial")
  "Default arguments to search"
  :type '(list string)
  :group 'org-fts)

(defvar org-fts/hits nil)
(defvar org-fts/args nil)
(defvar org-fts/timer (run-with-idle-timer (* 60 5) t 'org-fts/idle-task))
(defvar org-fts/actual-program nil)

(defun org-fts/check-db ()
  "create fts db if it is not there"
  (and (org-fts/test org-fts/db "need to customize org-fts/db, it has no value")
       (org-fts/ensure-binary)
       (or (file-exists-p org-fts/db)
           (let ((ret (call-process org-fts/actual-program nil nil nil
                                    "create" org-fts/db)))
             (org-fts/test (eql 0 ret) "creating database %s returned error %s" org-fts/db ret)))))

(defun org-fts/test (item &rest format)
  "print a message if a test fails"
  (or item
      (progn
        (apply 'message format)
        nil)))

(defun org-fts/hook ()
  "A file was just saved, index/reindex it if it's an org-mode file"
  (when (and
         (buffer-file-name)
	     (equal major-mode 'org-mode)
         (org-fts/check-db))
    (message "UPDATING ORG-FTS: %s" (file-truename (buffer-file-name)))
    (start-process "org-fts" nil org-fts/actual-program
                   "input" "-org" org-fts/db (file-truename (buffer-file-name)))))

(add-hook 'after-save-hook 'org-fts/hook)

(defun org-fts/idle-task ()
  "After a certain amount of idle time, update the org-fts database"
  (when (org-fts/check-db)
    (let ((process (start-process "org-fts" nil org-fts/actual-program
                                  "update" org-fts/db)))
      (set-process-sentinel process
                            (lambda (process status)
                              (ignore process)
                              (when (equal status "finished\n")
                                (start-process "org-fts" nil org-fts/actual-program
                                               "compact" org-fts/db)))))))

(defun org-fts/microfts-search (termStr)
  (let ((terms (condition-case nil (split-string-and-unquote termStr) ((debug error) nil))))
    (when (and (org-fts/test terms "EMPTY TERMS") (org-fts/check-db))
      (with-current-buffer (get-buffer-create "org-search")
        (erase-buffer)
        (let ((ret (apply #'call-process org-fts/actual-program nil "org-search" nil
                          `("search" "-sexp" ,@org-fts/search-args ,org-fts/db ,@terms))))
          (if (not (eql ret 0))
              (progn
                (message "Search returned error %s" ret)
                nil)
            (let ((lines (mapcar 'read (split-string (buffer-string) "\n" t))))
              (setq org-fts/args terms)
              (setq org-fts/hits lines)
              (mapcar (lambda (line)
                        (let* ((file (plist-get line :filename))
                               (text (replace-regexp-in-string "\n" "\\\\n" (plist-get line :text))))
                          (format "%s:%s: %s" (file-name-base file) (plist-get line :line)  text)))
                      lines)
              )))))))

(defun org-fts/found (arg)
  (ignore arg)
  (let ((hit (elt org-fts/hits ivy--index)))
    (find-file (plist-get hit :filename))
    (if (equal major-mode 'org-mode) (org-show-all))
    (goto-char (1+ (plist-get hit :char-offset)))))

(defvar org-fts/history nil)

(defun org-fts/display (line)
  (let* ((colon1 (cl-search ":" line))
         (colon2 (cl-search ":" line :start2 (1+ colon1)))
         (text (1+ colon2))
         (lowLn (downcase line)))
    (add-text-properties 0 colon1 '(face ivy-grep-info) line)
    (add-text-properties (1+ colon1) colon2 '(face ivy-grep-line-number) line)
    (cl-do ((a 0 (cl-incf a))) ((>= a (length org-fts/args)))
      (let* ((arg (downcase (elt org-fts/args a))))
        (cl-do ((i (cl-search arg lowLn :start2 text) (cl-search arg lowLn :start2 (+ i (length arg)))))
            ((not i))
          (put-text-property i (+ i (length arg)) 'face 'ivy-minibuffer-match-face-2 line))))
    line))

(ivy-configure 'fts
  :display-transformer-fn 'org-fts/display)

(defun org-fts/search ()
  "Perform an fts search"
  (interactive)
  (when (org-fts/check-db)
    (call-process org-fts/actual-program nil nil nil
                  "update" org-fts/db)
    (ivy-read "Org search: " 'org-fts/microfts-search
              :history 'org-fts/history
              :dynamic-collection t
              :action 'org-fts/found
              :caller 'fts)))

(defun org-fts/ensure-binary ()
  "Make sure microfts is present"
  (setq org-fts/actual-program
        (or
         (and org-fts/program
              (or (file-exists-p org-fts/program)
                  (error "Program not found: %s" org-fts/program))
              (or (file-executable-p org-fts/program)
                  (error "Program exists but is not executable: %s" org-fts/program))
              org-fts/program)
         (and (file-exists-p org-fts/baseprogram)
              (or (file-executable-p org-fts/baseprogram)
                  (error "Program exists but is not executable: %s" org-fts/baseprogram))
              org-fts/baseprogram)
         ;; if baseprogram is not there, download it
         (let* ((url (cdr (assoc system-type org-fts/microfts-url-alist)))
                (comprfile (format "%s.gz" org-fts/baseprogram))
                (compr auto-compression-mode))
           (when (not org-fts/baseprogram) (error "No value for org-fts/baseprogram or org-fts/program"))
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
                   (rename-file comprfile org-fts/baseprogram)
                   (kill-buffer)
                   org-fts/baseprogram)
               (if compr (auto-compression-mode 1))))))))

(provide 'org-fts)

;;; org-fts.el ends here
