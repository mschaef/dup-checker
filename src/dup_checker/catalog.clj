(ns dup-checker.catalog
  (:use dup-checker.util
        sql-file.sql-util)
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [clj-commons.digest :as digest]
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

(defn- insert-catalog [ catalog-rec ]
    (:catalog_id (first
                  (jdbc/insert! (sfm/db) :catalog catalog-rec))))

(defn- create-catalog [ catalog-name root-path catalog-type]
  (insert-catalog {:name catalog-name
                   :catalog_type_id (find-catalog-type-id catalog-type)
                   :created_on (java.util.Date.)
                   :updated_on (java.util.Date.)
                   :root_path root-path
                   :hostname (current-hostname)}) )

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

(defn- get-all-catalog-files [ catalog-id ]
  (query-all (sfm/db)
             [(str "SELECT *"
                   "  FROM file"
                   " WHERE file.catalog_id = ?"
                   " ORDER BY md5_digest")
              catalog-id]))

(defn- get-catalog-file-names [ catalog-id ]
  (set (map :name (get-all-catalog-files catalog-id))))

(def image-extensions
  #{"vob" "m4p" "wmv" "xbm" "lrcat" "pcx"
    "dng" "fig" "psd" "jpeg" "hdr" "mpeg" "mpg" "xmp"
    "wma" "xpm" "moi" "mom" "sbr" "mov" "dvi" "tga"
    "svg" "tsp" "mod" "avi" "mp4" "xcf" "tif" "bmp"
    "mp3" "pdf" "arw" "ithmb" "gif" "nef" "png" "jpg"
    "mts" "heic"})

(defn- file-md5-digest [ file-info ]
  (with-retries
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
  (let [catalog-files (get-catalog-file-names catalog-id)]
    (doseq [ f file-infos ]
      (catalog-file catalog-files catalog-id f))))

(defn- cmd-list-catalogs
  "List all catalogs"
  [ ]
  (table
   [:n :catalog_id :name :root_path :catalog_type :size :updated_on]
   (query-all (sfm/db)
              [(str "SELECT catalog.catalog_id, catalog.name, catalog.root_path, catalog.updated_on, count(file_id) as n, sum(file.size) as size, catalog_type.catalog_type"
                    "  FROM catalog, file, catalog_type"
                    " WHERE catalog.catalog_id = file.catalog_id"
                    "   AND catalog.catalog_type_id = catalog_type.catalog_type_id"
                    " GROUP BY catalog.catalog_id, catalog.name, catalog.updated_on, catalog_type, catalog.root_path"
                    " ORDER BY catalog_id")])))

(defn- get-required-catalog-id [ catalog-name ]
  (or (get-catalog-id catalog-name)
      (fail "No known catalog: " catalog-name)))

(defn- cmd-list-catalog-files
  "List all files present in a catalog."
  [ catalog-name ]

  (table
   [:md5_digest :name :size]
   (get-all-catalog-files (get-required-catalog-id catalog-name))))

(defn- cmd-remove-catalog
  "Remove a catalog."
  [ catalog-name ]

  (let [catalog-id (or (get-catalog-id catalog-name)
                       (fail "No known catalog: " catalog-name))]
    (jdbc/delete! (sfm/db) :file [ "catalog_id=?" catalog-id])
    (jdbc/delete! (sfm/db) :catalog [ "catalog_id=?" catalog-id])))

(defn- cmd-export-catalog
  "Export a catalog to an EDN file."

  [ catalog-name filename ]
  (let [catalog-id (get-required-catalog-id catalog-name)]
    (pretty-spit
     filename
     {:catalog (query-first (sfm/db)
                            [(str "SELECT name, created_on, updated_on, root_path, hostname, catalog_type"
                                  "  FROM catalog, catalog_type"
                                  " WHERE catalog_id=?"
                                  "   AND catalog.catalog_type_id = catalog_type.catalog_type_id")
                             catalog-id])
      :items (get-all-catalog-files catalog-id)})))

(defn- cmd-import-catalog
  "Import a catalog previously exported to an EDN file."
  [ catalog-name filename ]

  (when (get-catalog-id catalog-name)
    (fail "Catalog already exists: " catalog-name))
  (let [{ catalog :catalog items :items } (pretty-slurp filename)
        catalog (-> catalog
                    (assoc :name catalog-name)
                    (dissoc :catalog_type)
                    (assoc :catalog_type_id (find-catalog-type-id (:catalog_type catalog))))
        catalog-id (insert-catalog catalog)]
    (log/info "Created catalog ID:" catalog-id)
    (doseq [ item items ]
      (log/info "Adding item: " (:name item))
      (jdbc/insert! (sfm/db)
                    :file
                    (assoc item :catalog_id catalog-id)))))

(defn- catalog-files-by-digest [ catalog-name ]
  (into {} (map (fn [ value ]
                  [(:md5_digest value) value])
                (get-all-catalog-files (get-required-catalog-id catalog-name)))))

(defn- cmd-catalog-list-missing
  "Identify files missing in a given catalog."

  [ catalog-name required-catalog-name ]

  (let [catalog-files (catalog-files-by-digest catalog-name) ]
    (table
     [:name :size :last_modified_on :md5_digest]
     (remove #(get catalog-files (:md5_digest %))
             (get-all-catalog-files
              (get-required-catalog-id required-catalog-name))))))

(def subcommands
  #^{:doc "Catalog subcommands"}
  {"ls" #'cmd-list-catalogs
   "list-files" #'cmd-list-catalog-files
   "rm" #'cmd-remove-catalog
   "list-missing" #'cmd-catalog-list-missing

   "export" #'cmd-export-catalog
   "import" #'cmd-import-catalog})
