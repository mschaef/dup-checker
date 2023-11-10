(ns dup-checker.core
  (:gen-class :main true)
  (:use playbook.core
        sql-file.sql-util)
  (:require [clojure.pprint :as pprint]
            [clj-commons.digest :as digest]
            [sql-file.core :as sql-file]
            [playbook.logging :as logging]
            [playbook.config :as config]
            [taoensso.timbre :as log]
            [clojure.java.jdbc :as jdbc]
            [again.core :as again]))

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
    {:file f
     :full-path path
     :extension (get-file-extension f)
     :last-modified-on (java.util.Date. (.lastModified f))
     :name (.substring path (+ 1 (count (.getCanonicalPath root))))
     :size (.length f)}))

;; Times in msec
(def retry-policy [ 0 5000 10000 15000 ])

(defn- compute-file-digests [ file-info ]
  (merge file-info
         ;;; MD5 only, because that's all the S3 API supports.
         {:md5-digest (again/with-retries retry-policy
                        ;;; Retry to accomodate potential I/O errors.
                        (digest/md5 (:file file-info)))}))

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

(def image-extensions
  #{"vob" "m4p" "wmv" "xbm" "tar" "gz" "lrcat" "pcx"
    "dng" "rtf" "fig" "psd" "jpeg" "hdr" "mpeg" "mpg" "xmp"
    "wma" "xpm" "moi" "mom" "sbr" "mov" "dvi" "tga" "tgz"
    "zip" "svg" "tsp" "mod" "avi" "mp4" "xcf" "tif" "bmp"
    "mp3" "pdf" "arw" "ithmb" "gif" "nef" "png" "jpg"})

(defn- catalog-file [ db-conn catalog-files catalog-id file-info ]
  (cond
    (catalog-files (:name file-info))
    (log/info "File already cataloged:" (:name file-info))

    (not (image-extensions (.toLowerCase (:extension file-info))))
    (log/info "Skipping non-image file:" (:name file-info))

    :else
    (let [ file-info (compute-file-digests file-info )]
      (log/info "Adding file to catalog:" (:name file-info))
      (jdbc/insert! db-conn
                    :file
                    {:name (:name file-info)
                     :catalog_id catalog-id
                     :extension (.toLowerCase (:extension file-info))
                     :size (:size file-info)
                     :last_modified_on (:last-modified-on file-info)
                     :md5_digest (:md5-digest file-info)}))))

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

(defn- current-hostname []
  (.getCanonicalHostName (java.net.InetAddress/getLocalHost)))

(defn- create-catalog [ db-conn catalog-name root-path ]
  (:catalog_id (first
                (jdbc/insert! db-conn
                              :catalog
                              {:name catalog-name
                               :created_on (java.util.Date.)
                               :updated_on (java.util.Date.)
                               :root_path root-path
                               :hostname (current-hostname)}))))

(defn- ensure-catalog [ db-conn catalog-name root-path ]
  (if-let [ existing-catalog-id (get-catalog-id db-conn catalog-name ) ]
    (update-catalog-date db-conn existing-catalog-id)
    (create-catalog db-conn catalog-name root-path)))

(defn- cmd-catalog-fs-files [ db-conn catalog-name root-path ]
  (let [catalog-id (ensure-catalog db-conn catalog-name root-path)
        root (clojure.java.io/file root-path)
        catalog-files (get-catalog-files db-conn catalog-id)]
    (doseq [f (filter #(.isFile %) (file-seq root))]
      (catalog-file db-conn catalog-files catalog-id (file-info root f)))))

(defn- cmd-list-catalogs [ db-conn]
  (pprint/print-table
   (map (fn [ catalog-rec ]
          {:n (:n catalog-rec)
           :name (:name catalog-rec)
           :size (:size catalog-rec)
           :updated-on (:updated_on catalog-rec)})
        (query-all db-conn
                   [(str "SELECT catalog.name, catalog.updated_on, count(file_id) as n, sum(file.size) as size"
                         "  FROM catalog, file"
                         " WHERE catalog.catalog_id = file.catalog_id"
                         " GROUP BY catalog.name, catalog.updated_on"
                         " ORDER BY name")]))))

(defn- cmd-list-catalog-files [ db-conn catalog-name ]
  (pprint/print-table
   (map (fn [ file-rec ]
          {:md5-digest (:md5_digest file-rec)
           :name (:name file-rec)
           :size (:size file-rec)})
        (let [catalog-id (or (get-catalog-id db-conn catalog-name)
                             (fail (str "No known catalog: " catalog-name)))]
          (query-all db-conn
                     [(str "SELECT md5_digest, name, size"
                           "  FROM file"
                           " WHERE catalog_id=?"
                           " ORDER BY md5_digest")
                      catalog-id])))))

(defn- cmd-list-dups [ db-conn ]
  (let [ result-set (query-all db-conn
                               [(str "SELECT md5_digest, count(md5_digest) as count"
                                     "  FROM file"
                                     " GROUP BY md5_digest"
                                     " ORDER BY count")])]
    (pprint/print-table
     (map (fn [ file-rec ]
            {:md5-digest (:md5_digest file-rec)
             :count (:count file-rec)})
          result-set))

    (println "n=" (count result-set))))

(defn- dispatch-subcommand [ db-conn args ]
  (if (= (count args) 0)
    (fail "Insufficient arguments.")
    (let [ [ subcommand & args ] args ]

      (case subcommand
        "lsc" (cmd-list-catalogs db-conn)
        "catalog" (cmd-catalog-fs-files db-conn
                                    (or (second args) "default")
                                    (or (first args) "."))
        "list" (cmd-list-catalog-files db-conn (or (first args) "default"))
        "list-dups" (cmd-list-dups db-conn)
        (fail "Unknown subcommand")))))

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
   :schemas [[ "dup-checker" 0 ]]})

(defn -main [& args] ;; TODO: Does playbook need a standard main? Or wrapper?
  (app-main
   (fn [ config args ]
     (sql-file/with-pool [db-conn (db-conn-spec config)]
       (dispatch-subcommand db-conn args)))
   args
   {:log-levels
    [[#{"hsqldb.*" "com.zaxxer.hikari.*"} :warn]
     [#{"dup-checker.*"} :info]]}))
