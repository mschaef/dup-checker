(ns dup-checker.catalog
  (:use playbook.core
        sql-file.sql-util
        dup-checker.util
        playbook.config)
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [clj-commons.digest :as digest]
            [clojure.java.jdbc :as jdbc]
            [sql-file.middleware :as sfm]))


(defprotocol AFileStore
  "A catalogable store for files."

  (get-store-files [ this ] "Return a sequence of files in the storee."))


(defn all-filenames-by-digest [ ]
  (into {} (map (fn [ value ]
                  [(:md5_digest value) (:name value)])
                (query-all (sfm/db) "SELECT md5_digest, name FROM file"))))

(defn catalog-filenames-by-digest [ catalog-id ]
  (into {} (map (fn [ value ]
                  [(:md5_digest value) (:name value)])
                (query-all (sfm/db)
                           ["SELECT md5_digest, name FROM file WHERE catalog_id=?"
                            catalog-id]))))

(defn- get-catalog-id [ catalog-name ]
  (query-scalar (sfm/db)
                [(str "SELECT catalog_id"
                      "  FROM catalog"
                      " WHERE name = ?")
                 catalog-name]))

(defn- get-catalog [ catalog-id ]
  (query-first (sfm/db)
               [(str "SELECT catalog.root_path, catalog_type.catalog_type"
                     "  FROM catalog, catalog_type"
                     " WHERE catalog_id = ?"
                     "   AND catalog.catalog_type_id = catalog_type.catalog_type_id")
                catalog-id]))

(defn- update-catalog-date [ existing-catalog-id ]
  (jdbc/update! (sfm/db)
                :catalog
                {:updated_on (java.util.Date.)}
                ["catalog_id=?" existing-catalog-id])
  existing-catalog-id)

(defn- find-catalog-type-id [ catalog-type ]
  (query-scalar-required
   (sfm/db)
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

(defn catalog-files [ catalog-id file-store ]
  (let [catalog-files (get-catalog-file-names catalog-id)]
    (doseq [ f (get-store-files file-store) ]
      (catalog-file catalog-files catalog-id f))))

(defn- query-catalogs []
  (query-all (sfm/db)
             [(str "SELECT catalog.catalog_id, catalog.name, catalog.root_path, catalog.created_on, catalog.updated_on, count(file_id) as n, sum(file.size) as size, catalog_type.catalog_type"
                   "  FROM catalog, file, catalog_type"
                   " WHERE catalog.catalog_id = file.catalog_id"
                   "   AND catalog.catalog_type_id = catalog_type.catalog_type_id"
                   " GROUP BY catalog.catalog_id, catalog.name, catalog.updated_on, catalog_type, catalog.root_path"
                   " ORDER BY catalog_id")]))

(defn- cmd-catalog-list
  "List all catalogs"
  [ ]
  (table
   [:name :created_on :updated_on [:catalog_uri 50] :n :size ]
   (map #(assoc % :catalog_uri (str (:catalog_type %) ":" (:root_path %)))
    (query-catalogs))))

(defn- get-required-catalog-id [ catalog-name ]
  (or (get-catalog-id catalog-name)
      (fail "No known catalog: " catalog-name)))

(defn- cmd-catalog-duplicates
  "List all duplicate files in a catalog by MD5 digest."
  [ catalog-name ]

  (let [catalog-id (get-required-catalog-id catalog-name)
        md5-to-filename (catalog-filenames-by-digest catalog-id)]
    (table
     [:md5_digest :count :name]
     (map #(assoc % :name (md5-to-filename (:md5_digest %)))
          (query-all (sfm/db)
                     [(str "SELECT * FROM ("
                           "   SELECT md5_digest, count(md5_digest) as count"
                           "     FROM file"
                           "   WHERE catalog_id=?"
                           "    GROUP BY md5_digest)"
                           " WHERE count > 1"
                           " ORDER BY count")
                      catalog-id])))))

(defn- cmd-catalog-list-files
  "List all files present in a catalog."
  [ catalog-name ]

  (table
   [:md5_digest :size :name]
   (get-all-catalog-files (get-required-catalog-id catalog-name))))

(defn- cmd-catalog-remove
  "Remove a catalog."
  [ catalog-name ]

  (let [catalog-id (get-required-catalog-id catalog-name)]
    (jdbc/delete! (sfm/db) :file [ "catalog_id=?" catalog-id])
    (jdbc/delete! (sfm/db) :catalog [ "catalog_id=?" catalog-id])))

(defn- cmd-catalog-set-root
  "Set the catalog root. (Useful if the cataloged files have been moved.)"
  [ catalog-name catalog-root ]

  (let [catalog-id (get-required-catalog-id catalog-name)]
      (jdbc/update! (sfm/db)
                    :catalog
                    {:root_path catalog-root}
                    ["catalog_id=?" catalog-id])))

(defn- cmd-catalog-export
  "Export a catalog to an EDN file."

  [ catalog-name filename ]
  (let [catalog-id (get-required-catalog-id catalog-name)]
    (edn-spit
     filename
     {:catalog (query-first (sfm/db)
                            [(str "SELECT name, created_on, updated_on, root_path, hostname, catalog_type"
                                  "  FROM catalog, catalog_type"
                                  " WHERE catalog_id=?"
                                  "   AND catalog.catalog_type_id = catalog_type.catalog_type_id")
                             catalog-id])
      :items (get-all-catalog-files catalog-id)})))

(defn- cmd-catalog-import
  "Import a catalog previously exported to an EDN file."
  [ catalog-name filename ]

  (when (get-catalog-id catalog-name)
    (fail "Catalog already exists: " catalog-name))
  (let [{ catalog :catalog items :items } (edn-slurp filename)
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
     [:last_modified_on :md5_digest :size :name ]
     (remove #(get catalog-files (:md5_digest %))
             (get-all-catalog-files
              (get-required-catalog-id required-catalog-name))))))

(defn- get-store [ scheme scheme-specific-part]
  (let [ store (or (cval :catalog-scheme scheme)
                   (fail "Unknown scheme: " scheme))]
    (store scheme-specific-part)))

(defn- cmd-catalog-create
  "Create a catalog rooted at a given URI."

  [ catalog-uri catalog-name ]
  (let [uri (java.net.URI. catalog-uri)
        scheme (.getScheme uri)
        scheme-specific-part (.getSchemeSpecificPart uri)]
    (catalog-files
     (ensure-catalog catalog-name scheme-specific-part scheme)
     (get-store scheme scheme-specific-part))))

(defn- cmd-catalog-update
  "Create a catalog rooted at a given URI."

  [ catalog-name ]

  (let [catalog-id (get-required-catalog-id catalog-name)
        {scheme :catalog_type
         scheme-specific-part :root_path} (get-catalog catalog-id)]
      (catalog-files catalog-id (get-store scheme scheme-specific-part))))

(def subcommands
  #^{:doc "Catalog subcommands"}
  {"ls" #'cmd-catalog-list
   "list-files" #'cmd-catalog-list-files
   "rm" #'cmd-catalog-remove
   "set-root" #'cmd-catalog-set-root

   "list-missing" #'cmd-catalog-list-missing
   "duplicates" #'cmd-catalog-duplicates

   "export" #'cmd-catalog-export
   "import" #'cmd-catalog-import

   "create" #'cmd-catalog-create
   "update" #'cmd-catalog-update})
