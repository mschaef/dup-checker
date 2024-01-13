(ns dup-checker.db
  (:use playbook.core
        sql-file.sql-util
        dup-checker.util)
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [clj-commons.digest :as digest]
            [sql-file.middleware :as sfm]
            [sql-file.core :as sfc
             ]))

(def date-format (java.text.SimpleDateFormat. "yyyyMMdd-hhmm"))

(defn- get-backup-filename [ ]
  (str "dc-backup-" (.format date-format (current-time)) ".tgz"))

(defn cmd-db-backup
  "Backup the working database to the current directory."

  []
  (let [ filename (get-backup-filename)]
    (println (str "Backing up to: " filename))
    (sfc/backup-to-file-online (sfm/db) filename)))

(defn cmd-db-shell
  "Start a sqltool shell connected to the working database."

  []
  (sfc/start-sqltool-shell (sfm/db)))

(def subcommands
  #^{:doc "Database administration commands"}
  {"backup" #'cmd-db-backup
   "shell" #'cmd-db-shell})
