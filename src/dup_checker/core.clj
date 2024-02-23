(ns dup-checker.core
  (:gen-class :main true)
  (:use playbook.core
        playbook.main
        dup-checker.util)
  (:require [playbook.config :as config]
            [sql-file.core :as sql-file]
            [sql-file.middleware :as sfm]
            [taoensso.timbre :as log]
            [playbook.config :as config]
            [dup-checker.catalog :as catalog]
            [dup-checker.describe :as describe]
            [dup-checker.fs :as fs]
            [dup-checker.gphoto :as gphoto]
            [dup-checker.s3 :as s3]
            [dup-checker.db :as db]))


(defn- cmd-run-script
  "Run a script or scripts."

  [ & file-name ]

  (doseq [ f file-name ]
    (log/report f)
    (load-file f)))

(def subcommands
  {"catalog" catalog/subcommands
   "db" db/subcommands
   "describe" describe/subcommands
   "gphoto" gphoto/subcommands
   "s3" s3/subcommands
   "run-script" cmd-run-script})

(defn- display-help [ cmd-map ]
  (println "\n\nCommand Paths:\n")

  (letfn [(display-subcommands [ cmd-map indent ]
            (doseq [ cmd-name (sort (keys cmd-map)) ]
              (let [cmd-fn (get cmd-map cmd-name)
                    { doc :doc arglists :arglists } (meta cmd-fn) ]
                (if (map? cmd-fn)
                  (do
                    (println (spaces (* indent 2)) cmd-name (if doc (str " - " doc) ""))
                    (display-subcommands cmd-fn (+ indent 1))
                    (println))
                  (println (spaces (* indent 2)) cmd-name (first arglists) (if doc (str " - " doc) ""))))))]
    (display-subcommands cmd-map 0)))

(defn- dispatch-subcommand [ cmd-map args ]
  (try
    (if (= (count args) 0)
      (fail "Insufficient arguments, missing subcommand.")
      (let [[ subcommand & args ] args]
        (if-let [ cmd-fn (get (assoc cmd-map "help" #(display-help cmd-map)) subcommand) ]
          (if (map? cmd-fn)
            (dispatch-subcommand cmd-fn args)
            (with-exception-barrier :command-processing
              (apply cmd-fn args)))
          (fail "Unknown subcommand: " subcommand))))
    (catch Exception e
      (display-help cmd-map))))

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
        (dispatch-subcommand subcommands args)))))
