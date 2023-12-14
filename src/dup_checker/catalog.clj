(ns dup-checker.catalog
  (:use dup-checker.util
        sql-file.sql-util)
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [clj-commons.digest :as digest]
            [again.core :as again]
            [clojure.java.jdbc :as jdbc]
            [sql-file.middleware :as sfm]))

(defn- get-catalog-id [catalog-name ]
  (query-scalar (sfm/db)
                [(str "SELECT catalog_id"
                      "  FROM catalog"
                      " WHERE name = ?")
                 catalog-name]))

(defn- update-catalog-date [ existing-catalog-id ]
  (jdbc/update! (sfm/db)
                :catalog
                {:updated_on (java.util.Date.)}
                ["catalog_id=?" existing-catalog-id])
  existing-catalog-id)

(defn- find-catalog-type-id [ catalog-type ]
  ;; TODO: query-scaler-required
  (query-scalar (sfm/db)
                [(str "SELECT catalog_type_id"
                      "  FROM catalog_type"
                      " WHERE catalog_type.catalog_type = ?")
                 catalog-type]))

(defn- create-catalog [ catalog-name root-path catalog-type]
  (:catalog_id (first
                (jdbc/insert! (sfm/db)
                              :catalog
                              {:name catalog-name
                               :catalog_type_id (find-catalog-type-id catalog-type)
                               :created_on (java.util.Date.)
                               :updated_on (java.util.Date.)
                               :root_path root-path
                               :hostname (current-hostname)}))))

(defn ensure-catalog [ catalog-name root-path catalog-type ]
  (if-let [ existing-catalog-id (get-catalog-id catalog-name ) ]
    (update-catalog-date existing-catalog-id)
    (create-catalog catalog-name root-path catalog-type)))

(defn- file-cataloged? [ catalog-id file-info ]
  (> (query-scalar (sfm/db)
                   [(str "SELECT COUNT(file_id)"
                         "  FROM file"
                         " WHERE file.name = ?"
                         "  AND file.catalog_id = ?")
                    (:name file-info)
                    catalog-id])
     0))

(defn get-catalog-files [ catalog-id ]
  (set (map :name (query-all (sfm/db)
                             [(str "SELECT name"
                                   "  FROM file"
                                   " WHERE file.catalog_id = ?")
                              catalog-id]))))

(def image-extensions
  #{"vob" "m4p" "wmv" "xbm" "tar" "gz" "lrcat" "pcx"
    "dng" "rtf" "fig" "psd" "jpeg" "hdr" "mpeg" "mpg" "xmp"
    "wma" "xpm" "moi" "mom" "sbr" "mov" "dvi" "tga" "tgz"
    "zip" "svg" "tsp" "mod" "avi" "mp4" "xcf" "tif" "bmp"
    "mp3" "pdf" "arw" "ithmb" "gif" "nef" "png" "jpg" "heic"})

;; Times in msec
(def retry-policy [ 0 5000 10000 15000 ])

(defn- file-md5-digest [ file-info ]
  (again/with-retries retry-policy
    (with-open [ r ((:data-stream-fn file-info)) ]
      (digest/md5 r))))

(defn- catalog-file [ catalog-files catalog-id file-info ]
  (cond
    (catalog-files (:name file-info))
    (log/info "File already cataloged:" (:name file-info))

    (not (image-extensions (.toLowerCase (:extension file-info))))
    (log/info "Skipping non-image file:" (:name file-info))

    :else
    (do
      (log/info "Adding file to catalog:" (:name file-info))
      (jdbc/insert! (sfm/db)
                    :file
                    {:name (:name file-info)
                     :catalog_id catalog-id
                     :extension (.toLowerCase (:extension file-info))
                     :size (:size file-info)
                     :last_modified_on (:last-modified-on file-info)
                     :md5_digest (file-md5-digest file-info)}))))

(defn catalog-files [ catalog-id file-infos ]
  (let [catalog-files (get-catalog-files catalog-id)]
    (doseq [ f file-infos ]
      (catalog-file catalog-files catalog-id f))))

(defn- cmd-list-catalogs
  "List all catalogs"
  [ ]
  (table
   (map (fn [ catalog-rec ]
          {:n (:n catalog-rec)
           :id (:catalog_id catalog-rec)
           :name (:name catalog-rec)
           :root-path (:root_path catalog-rec)
           :catalog-type (:catalog_type catalog-rec)
           :size (:size catalog-rec)
           :updated-on (:updated_on catalog-rec)})
        (query-all (sfm/db)
                   [(str "SELECT catalog.catalog_id, catalog.name, catalog.root_path, catalog.updated_on, count(file_id) as n, sum(file.size) as size, catalog_type.catalog_type"
                         "  FROM catalog, file, catalog_type"
                         " WHERE catalog.catalog_id = file.catalog_id"
                         "   AND catalog.catalog_type_id = catalog_type.catalog_type_id"
                         " GROUP BY catalog.catalog_id, catalog.name, catalog.updated_on, catalog_type, catalog.root_path"
                         " ORDER BY catalog_id")]))))

(defn- cmd-list-catalog-files
  "List all files present in a catalog."
  [ catalog-name ]

  (table
   (map (fn [ file-rec ]
          {:md5-digest (:md5_digest file-rec)
           :name (:name file-rec)
           :size (:size file-rec)})
        (let [catalog-id (or (get-catalog-id catalog-name)
                             (fail "No known catalog: " catalog-name))]
          (query-all (sfm/db)
                     [(str "SELECT md5_digest, name, size"
                           "  FROM file"
                           " WHERE catalog_id=?"
                           " ORDER BY md5_digest")
                      catalog-id])))))

(defn- cmd-remove-catalog
  "Remove a catalog."
  [ catalog-name ]

  (let [catalog-id (or (get-catalog-id catalog-name)
                       (fail "No known catalog: " catalog-name))]
    (jdbc/delete! (sfm/db) :file [ "catalog_id=?" catalog-id])
    (jdbc/delete! (sfm/db) :catalog [ "catalog_id=?" catalog-id])))

(def subcommands
  #^{:doc "Catalog subcommands"}
  {"ls" #'cmd-list-catalogs
   "list-files" #'cmd-list-catalog-files
   "rm" #'cmd-remove-catalog})
