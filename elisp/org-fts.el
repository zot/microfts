;;;;;
;; org-fts.el
;; (c) 2020 by Bill Burdick
;; Shared under MIT license
;;
(require 'cl-lib)
(require 'seq)
(require 'org)

(defgroup org-fts ()
  "Customization for org-fts")

(defcustom org-fts/program "microfts"
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

(defun org-fts/check-db ()
  "create fts db if it is not there"
  (and (org-fts/test org-fts/db "need to customize org-fts/db, it has no value")
       (org-fts/test org-fts/program "need to customize org-fts/program, it has no value")
       (or (file-exists-p org-fts/db)
           (let ((ret (call-process org-fts/program nil nil nil
                                    "create" org-fts/db)))
             (org-fts/test (eql 0 ret) "creating database %s returned error %s" org-fts/db ret)))))

(defun org-fts/test (item &rest format)
  "print a message if a test fails"
  (or item
      (progn
        (apply 'message format)
        nil)))

(defun org-fts/hook ()
  "An org file was just saved, index/reindex it"
  (message "ORG-FTS: %s input %s %s" org-fts/program org-fts/db (file-truename (buffer-file-name)))
  (when (and
         (buffer-file-name)
	     (or (f-ext? (buffer-file-name) "org")
             (f-ext? (buffer-file-name) "org_archive"))
         (org-fts/check-db))
    (start-process "org-fts" nil org-fts/program
                   "input" "-org" org-fts/db (file-truename (buffer-file-name)))))

(add-hook 'org-mode-hook 'org-fts/hook)

(defun org-fts/idle-task ()
  "After a certain amount of idle time, update the org-fts database"
  (when (org-fts/check-db)
    (start-process "org-fts" nil org-fts/program
                   "update" org-fts/db)))

(defun org-fts/microfts-search (termStr)
  (let ((terms (condition-case nil (split-string-and-unquote termStr) ((debug error) nil))))
    (when (and (org-fts/test terms "EMPTY TERMS") (org-fts/check-db))
      (save-excursion
        (set-buffer (get-buffer-create "org-search"))
        (erase-buffer)
        (let ((ret (apply #'call-process org-fts/program nil "org-search" nil
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
    (do ((a 0 (incf a))) ((>= a (length org-fts/args)))
      (let* ((arg (downcase (elt org-fts/args a))))
        (do ((i (cl-search arg lowLn :start2 text) (cl-search arg lowLn :start2 (+ i (length arg)))))
            ((not i))
          (put-text-property i (+ i (length arg)) 'face 'ivy-minibuffer-match-face-2 line))))
    line))

(ivy-configure 'fts
  :display-transformer-fn 'org-fts/display)

(defun org-fts/search ()
  "Perform an fts search"
  (interactive)
  (when (org-fts/check-db)
    (call-process org-fts/program nil nil nil
                  "update" org-fts/db)
    (ivy-read "Org search: " 'org-fts/microfts-search
              :history 'org-fts/history
              :dynamic-collection t
              :action 'org-fts/found
              :caller 'fts)))
