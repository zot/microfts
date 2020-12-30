;;; ivy-org-fts.el --- Ivy support for org-fts -*- lexical-binding: t; -*-

;; (c) 2020 by Bill Burdick, shared under MIT license

;; Author: Bill Burdick <bill.burdick@gmail.com>
;; URL: https://github.com/zot/microfts/tree/main/elisp/ivy-org-fts.el
;; Version: 1.0.0
;; Package-Requires: ((org-fts "1.0.0") (ivy "0.13.1") (emacs "26.0"))
;; Keywords: org convenience ivy

;;; Commentary:

;; Add ivy support for org-fts

;; Thanks to Professor John Kitchin for testing and contributions

;;; Code:

(require 'org-fts)
(require 'ivy)

(defvar ivy-org-fts-hits nil)
(defvar ivy-org-fts-args nil)
(defvar ivy-org-fts-history nil)
(defvar ivy-org-fts-file-history nil)
(defvar ivy-org-fts-file-switch nil)
(defconst ivy-org-fts-keymap
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-f")
      (lambda ()
        (interactive)
        (setq ivy-org-fts-file-switch (not (member "-file" org-fts-search-args)))
        (if ivy-org-fts-file-switch
            (add-to-list 'org-fts-search-args "-file")
          (setq org-fts-search-args (remove "-file" org-fts-search-args)))
        ;; this is hackery to get the candidates to refresh
        (ivy-update-candidates (ivy-org-fts-microfts-search ivy-text))))
    map))

(defun ivy-org-fts-found (arg)
  "Visit result ARG from an ivy-org-fts search."
  (ignore arg)
  (let ((hit (elt ivy-org-fts-hits ivy--index)))
    (find-file (plist-get hit :filename))
    (if (equal major-mode 'org-mode) (org-show-all))
    (goto-char (point-min))
    (forward-line (- (plist-get hit :line) 1))
    (forward-char (plist-get hit :offset))))

(defun ivy-org-fts-display (line)
  "Prepare LINE for display in ivy."
  (let* ((colon1 (cl-search ":" line))
         (colon2 (cl-search ":" line :start2 (1+ colon1)))
         (text (1+ colon2))
         (lowLn (downcase line)))
    (add-text-properties 0 colon1 '(face ivy-grep-info) line)
    (add-text-properties (1+ colon1) colon2 '(face ivy-grep-line-number) line)
    (cl-do ((a 0 (cl-incf a))) ((>= a (length ivy-org-fts-args)))
      (let* ((arg (downcase (elt ivy-org-fts-args a))))
        (cl-do ((i (cl-search arg lowLn :start2 text) (cl-search arg lowLn :start2 (+ i (length arg)))))
            ((not i))
          (put-text-property i (+ i (length arg)) 'face 'ivy-minibuffer-match-face-2 line))))
    line))

(ivy-configure 'fts
  :display-transformer-fn 'ivy-org-fts-display)

(defun ivy-org-fts-microfts-search (term-str)
  "Call microfts to search for TERM-STR from ivy."
  (let* ((terms (org-fts-split-terms term-str))
         (lines (org-fts-microfts-basic-search terms)))
    (setq ivy-org-fts-args terms)
    (setq ivy-org-fts-hits lines)
    (mapcar (lambda (line)
              (let* ((file (plist-get line :filename))
                     (text (replace-regexp-in-string "\n" "\\\\n" (plist-get line :text))))
                (format "%s:%s: %s" (file-name-base file) (plist-get line :line)  text)))
            lines)))

(defun ivy-org-fts-search (&optional file-match)
  "Perform an fts search.
if FILE-MATCH is non-nil search at the file level."
  (interactive "P")
  (let ((org-fts-search-args org-fts-search-args))
    (when file-match (add-to-list 'org-fts-search-args "-file"))
    (when (org-fts-check-db)
      (call-process org-fts-actual-program nil nil nil
                    "update" org-fts-db)
      (ivy-read "Org search: " 'ivy-org-fts-microfts-search
                :history 'ivy-org-fts-history
                :dynamic-collection t
                :action 'ivy-org-fts-found
                :keymap ivy-org-fts-keymap
                :caller 'fts))))

(defun ivy-org-fts-find-org-file ()
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
        (ivy-read "Find org file: " (seq-sort (lambda (a b)
                                                (string-collate-lessp
                                                 (downcase a) (downcase b))) files)
                  :history ivy-org-fts-file-history
                  :action 'find-file)))))

(provide 'ivy-org-fts)

;;; ivy-org-fts.el ends here
