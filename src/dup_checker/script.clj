(ns dup-checker.script
  (:gen-class :main true)
  (:use playbook.core
        playbook.main
        dup-checker.util)
  (:require [taoensso.timbre :as log]
            [dup-checker.catalog :as catalog]
            [dup-checker.describe :as describe]
            [dup-checker.fs :as fs]
            [dup-checker.gdrive :as gdrive]
            [dup-checker.s3 :as s3]
            [dup-checker.db :as db]))

(defn cmd-run-script
  "Run a script or scripts."

  [ & file-name ]

  (doseq [ f file-name ]
    (log/report "Running Script:" f)
    (binding [ *ns* (find-ns 'dup-checker.script)]
      (load-file f))))
