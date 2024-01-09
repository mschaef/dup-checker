(ns dup-checker.core
  (:gen-class :main true)
  (:use playbook.core
        sql-file.sql-util
        dup-checker.util)
  (:require [clojure.pprint :as pprint]
            [sql-file.core :as sql-file]
            [sql-file.middleware :as sfm]
            [playbook.logging :as logging]
            [playbook.config :as config]
            [taoensso.timbre :as log]
            [clojure.java.jdbc :as jdbc]
            [dup-checker.catalog :as catalog]
            [dup-checker.describe :as describe]
            [dup-checker.fs :as fs]
            [dup-checker.gphoto :as gphoto]
            [dup-checker.s3 :as s3]))

(def subcommands
  {"s3" s3/subcommands
   "catalog" catalog/subcommands
   "gphoto" gphoto/subcommands
   "fs" fs/subcommands
   "describe" describe/subcommands})

(defn- spaces [ n ]
  (clojure.string/join (repeat n " ")))

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

(defn- app-main
  ([ entry args config-overrides ]
   (let [config (-> (config/load-config)
                    (merge config-overrides))]
     (logging/setup-logging config)
     (log/info "Starting App" (:app config))
     (with-exception-barrier :app-entry
       (entry config args))
     (log/info "end run.")))

  ([ entry args ]
   (app-main entry {:log-levels
                    [[#{"hsqldb.*" "com.zaxxer.hikari.*"} :warn]]})))

(defn- db-conn-spec [ config ]
  ;; TODO: Much of this logic should somehow go in playbook
  {:name (or (config-property "db.subname")
             (get-in config [:db :subname] "dup-checker"))
   :schema-path [ "sql/" ]
   :schemas [[ "dup-checker" 5 ]]})

(defn -main [& args] ;; TODO: Does playbook need a standard main? Or wrapper?
  (app-main
   (fn [ config args ]
     (sql-file/with-pool [db-conn (db-conn-spec config)]
       (sfm/with-db-connection db-conn
         (dispatch-subcommand subcommands args))))
   args
   {:log-levels
    [[#{"hsqldb.*" "com.zaxxer.hikari.*"} :warn]
     [#{"dup-checker.*"} :info]]}))
