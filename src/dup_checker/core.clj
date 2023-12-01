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
            [dup-checker.fs :as fs]
            [dup-checker.gphoto :as gphoto]
            [dup-checker.s3 :as s3]))

(defn- filenames-by-digest [ ]
  (into {} (map (fn [ value ]
                  [(:md5_digest value) (:name value)])
                (query-all (sfm/db)
                           [(str "SELECT md5_digest, name"
                                 "  FROM file")]))))

(defn- cmd-list-dups
  "List all duplicate files by MD5 digest."
  [ ]

  (let [ md5-to-filename (filenames-by-digest)]
    (table
     (map (fn [ file-rec ]
            {:md5-digest (:md5_digest file-rec)
             :count (:count file-rec)
             :name (md5-to-filename (:md5_digest file-rec))})
          (query-all (sfm/db)
                     [(str "SELECT * FROM ("
                           "   SELECT md5_digest, count(md5_digest) as count"
                           "     FROM file"
                           "    GROUP BY md5_digest)"
                           " WHERE count > 1"
                           " ORDER BY count")])))))

(defn- cmd-describe-file
  "Describe a file identified by MD5 digest."
  [ md5-digest ]
  (table
   (query-all (sfm/db)
              [(str "SELECT *"
                    "  FROM file"
                    " WHERE md5_digest=?"
                    " ORDER BY name")
               md5-digest])))

(def subcommands
  {"s3" s3/subcommands
   "catalog" catalog/subcommands
   "gphoto" gphoto/subcommands
   "fs" fs/subcommands
   "describe" #'cmd-describe-file
   "list-dups" #'cmd-list-dups})

(defn- display-help [ cmd-map ]
  (println "Valid Commands:")
  (table
   (map (fn [ cmd-name ]
          {:command cmd-name
           :help (:doc (meta (get cmd-map cmd-name)))
           :args (:arglists (meta (get cmd-map cmd-name)))})
        (sort (keys cmd-map)))))

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
   :schemas [[ "dup-checker" 2 ]]})


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
