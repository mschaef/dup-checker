(ns dup-checker.core
  (:gen-class :main true)
  (:use playbook.core
        playbook.main
        dup-checker.util)
  (:require [playbook.config :as config]
            [sql-file.core :as sql-file]
            [sql-file.middleware :as sfm]
            [taoensso.timbre :as log]
            [dup-checker.catalog :as catalog]
            [dup-checker.describe :as describe]
            [dup-checker.fs :as fs]
            [dup-checker.gdrive :as gdrive]
            [dup-checker.s3 :as s3]
            [dup-checker.db :as db]
            [dup-checker.script :as script]
            [dup-checker.cli :as cli]))

(def subcommands
  {"catalog" catalog/subcommands
   "db" db/subcommands
   "describe" describe/subcommands
   "gdrive" gdrive/subcommands
   "s3" s3/subcommands
   "run-script" #'script/cmd-run-script})

(defn- db-conn-spec [ ]
  ;; TODO: Much of this logic should somehow go in playbook
  {:name (or (config/property "db.subname")
             (or (config/cval :db :subname)
                 (config/cval :app :name)))
   :schema-path [ "sql/" ]
   :schemas (config/cval :db :schemas)})

(def store-schemes
  {"fs" fs/get-store
   "s3" s3/get-store})

(defmain [& args]
  (config/with-extended-config {:store-scheme store-schemes}
    (sql-file/with-pool [db-conn (db-conn-spec)]
      (sfm/with-db-connection db-conn
        (cli/dispatch-subcommand subcommands args)))))
