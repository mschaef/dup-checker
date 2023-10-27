(ns dup-checker.core
  (:use playbook.core
        sql-file.sql-util)
  (:require [clj-commons.digest :as digest]
            [sql-file.core :as sql-file]
            [playbook.logging :as logging]
            [playbook.config :as config]
            [taoensso.timbre :as log]
            [clojure.java.jdbc :as jdbc]))

(defn- fail [ message ]
  (throw (RuntimeException. message)))

(defn- get-file-extension [ f ]
  (let [name (.getName f)
        sep-index (.lastIndexOf name ".")]
    (if (< sep-index 0)
      name
      (.substring name (+ 1 sep-index)))))

(defn- file-info [ root f ]
  (let [path (.getCanonicalPath f)]
    {:full-path path
     :extension (get-file-extension f)
     :last-modified-on (java.util.Date. (.lastModified f))
     :name (.substring path (+ 1 (count (.getCanonicalPath root))))
     :size (.length f)}))

(defn- compute-file-digests [ file-info ]
  (merge file-info
         {:md5-digest (digest/md5 (:full-path file-info))
          :sha256-digest (digest/sha256 (:full-path file-info))}))

(defn- file-cataloged? [ db-conn catalog-id file-info ]
  (> (query-scalar db-conn
                   [(str "SELECT COUNT(file_id)"
                         "  FROM file"
                         " WHERE file.name = ?"
                         "  AND file.catalog_id = ?")
                    (:name file-info)
                    catalog-id])
     0))

(defn- get-catalog-files [ db-conn catalog-id ]
  (set (map :name (query-all db-conn
                             [(str "SELECT name"
                                   "  FROM file"
                                   " WHERE file.catalog_id = ?")
                              catalog-id]))))

(defn- catalog-file [ db-conn catalog-files catalog-id file-info ]
  (if (catalog-files (:name file-info))
    (log/info "File already cataloged:" (:name file-info))
    (let [ file-info (compute-file-digests file-info )]
      (log/info "Adding file to catalog:" (:name file-info))
      (jdbc/insert! db-conn
                    :file
                    {:name (:name file-info)
                     :catalog_id catalog-id
                     :extension (:extension file-info)
                     :size (:size file-info)
                     :last_modified_on (:last-modified-on file-info)
                     :md5_digest (:md5-digest file-info)
                     :sha256_digest (:sha256-digest file-info)}))))

(defn- get-catalog-id [ db-conn catalog-name ]
  (query-scalar db-conn
                [(str "SELECT catalog_id"
                      "  FROM catalog"
                      " WHERE name = ?")
                 catalog-name]))

(defn- update-catalog-date [ db-conn existing-catalog-id ]
  (jdbc/update! db-conn :catalog
                {:updated_on (java.util.Date.)}
                ["catalog_id=?" existing-catalog-id])
  existing-catalog-id)

(defn- create-catalog [ db-conn catalog-name ]
  (:catalog_id (first
                (jdbc/insert! db-conn
                              :catalog
                              {:name catalog-name
                               :created_on (java.util.Date.)
                               :updated_on (java.util.Date.)}))))

(defn- ensure-catalog [ db-conn catalog-name ]
  (if-let [ existing-catalog-id (get-catalog-id db-conn catalog-name ) ]
    (update-catalog-date db-conn existing-catalog-id)
    (create-catalog db-conn catalog-name)))

(defn- catalog-fs-files [ db-conn catalog-name root-path ]
  (let [catalog-id (ensure-catalog db-conn catalog-name)
        root (clojure.java.io/file root-path)
        catalog-files (get-catalog-files db-conn catalog-id)]
    (doseq [f (filter #(.isFile %) (file-seq root))]
      (catalog-file db-conn catalog-files catalog-id (file-info root f)))))

(defn- list-catalogs [ db-conn]
  (doseq [ catalog-rec (query-all db-conn
                               [(str "SELECT catalog.name, catalog.updated_on, count(file_id) as size"
                                     "  FROM catalog, file"
                                     " WHERE catalog.catalog_id = file.catalog_id"
                                     " GROUP BY catalog.name, catalog.updated_on"
                                     " ORDER BY name")])]
      (println (:name catalog-rec) " " (:size catalog-rec) " " (:updated_on catalog-rec))))

(defn- show-file-report [ db-conn catalog-name ]
  (let [catalog-id (or (get-catalog-id db-conn catalog-name)
                       (fail (str "No known catalog: " catalog-name)))]

    (doseq [ file-rec (query-all db-conn
                                 [(str "SELECT md5_digest, name, size"
                                       "  FROM file"
                                       " WHERE catalog_id=?"
                                       " ORDER BY md5_digest")
                                  catalog-id])]
      (println (:md5_digest file-rec) " " (:name file-rec) "(" (:size file-rec) ")"))))

(defn- db-conn-spec [ config ]
  ;; TODO: Much of this logic should somehow go in playbook
  {:name (or (config-property "db.subname")
             (get-in config [:db :subname] "dup-checker"))
   :schema-path [ "sql/" ]
   :schemas [[ "dup-checker" 0 ]]})

(defn- dispatch-subcommand [ db-conn args ]
  (if (= (count args) 0)
    (fail "Insufficient arguments.")
    (let [ [ subcommand & args ] args]
      (case subcommand
        "lsc" (list-catalogs db-conn)
        "catalog" (catalog-fs-files db-conn
                                    (or (second args) "default")
                                    (or (first args) "."))
        "show" (show-file-report db-conn (or (first args) "default"))
        (fail "Unknown subcommand")))))

(defn -main [& args] ;; TODO: Does playbook need a standard main? Or wrapper?
  (let [config (-> (config/load-config)
                   (assoc :log-levels
                          [[#{"hsqldb.*" "com.zaxxer.hikari.*"} :warn]
                           [#{"dup-checker.*"} :info]]))]
    (logging/setup-logging config)
    (log/info "Starting App" (:app config))

    (sql-file/with-pool [db-conn (db-conn-spec config)]
      (dispatch-subcommand db-conn args))
    (log/info "end run.")))
