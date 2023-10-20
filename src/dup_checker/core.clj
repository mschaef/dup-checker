(ns dup-checker.core
  (:use playbook.core
        sql-file.sql-util)
  (:require [clj-commons.digest :as digest]
            [sql-file.core :as sql-file]
            [playbook.logging :as logging]
            [playbook.config :as config]
            [taoensso.timbre :as log]
            [clojure.java.jdbc :as jdbc]))

(defn- file-info [ root f ]
  (let [path (.getCanonicalPath f)]
    {:full-path path
     :name (.substring path (+ 1 (count (.getCanonicalPath root))))
     :size (.length f)}))

(defn- compute-file-digests [ file-info ]
  (merge file-info
         {:md5-digest (digest/md5 (:full-path file-info))
          :sha256-digest (digest/sha256 (:full-path file-info))}))

(defn- file-cataloged? [ db-conn file-info ]
  (> (query-scalar db-conn
                   [(str "SELECT COUNT(file_id)"
                         "  FROM file"
                         " WHERE file.name = ?")
                    (:name file-info)])
     0))

(defn- catalog-file [ db-conn file-info ]
  (if (file-cataloged? db-conn file-info)
    (log/info "File already cataloged:" (:name file-info))
    (let [ file-info (compute-file-digests file-info )]
      (log/info "Adding file to catalog:" (:name file-info))
      (jdbc/insert! db-conn
                    :file
                    {:name (:name file-info)
                     :size (:size file-info)
                     :md5_digest (:md5-digest file-info)
                     :sha256_digest (:sha256-digest file-info)}))))

(defn- list-files [ db-conn root-path ]
  (let [root (clojure.java.io/file root-path)]
    (doseq [f (filter #(.isFile %) (file-seq root))]
      (catalog-file db-conn (file-info root f)))))

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
      (list-files db-conn "."))
    (log/info "end run.")))
