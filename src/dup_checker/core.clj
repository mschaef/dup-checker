(ns dup-checker.core
  (:use playbook.core)
  (:require [clj-commons.digest :as digest]
            [sql-file.core :as sql-file]
            [playbook.logging :as logging]
            [playbook.config :as config]
            [taoensso.timbre :as log]))

(defn- file-info [ f ]
  {:name (.getAbsolutePath f)
   :size (.length f)
   :md5-digest (digest/md5 f)
   :sha256-digest (digest/sha256 f)})

(defn- list-files [ db-conn ]
  (let [root (clojure.java.io/file ".")]
    (doseq [f (filter #(.isFile %) (file-seq root))]
      (log/info (file-info f)))))

(defn- db-conn-spec [ config ]
  ;; TODO: Much of this logic should somehow go in playbook
  {:name (or (config-property "db.subname")
             (get-in config [:db :subname] "dup-checker"))
   :schema-path [ "sql/" ]
   :schemas [[ "dup-checker" 0 ]]})

(defn -main [& args] ;; TODO: Does playbook need a standard main? Or wrapper?
  (let [config (-> (config/load-config)
                   (assoc :log-levels
                          [[#{"hsqldb.*" "com.zaxxer.hikari.*"} :warn]
                           [#{"dup-checker.*"} :info]]))]
    (logging/setup-logging config)
    (log/info "Starting App" (:app config))
    (sql-file/with-pool [db-conn (db-conn-spec config)]
      (list-files db-conn))
    (log/info "end run.")))
