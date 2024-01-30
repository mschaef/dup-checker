(ns dup-checker.catalog
  (:use playbook.core
        sql-file.sql-util
        dup-checker.util
        playbook.config)
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [clj-commons.digest :as digest]
            [clojure.java.jdbc :as jdbc]
            [sql-file.middleware :as sfm]
            [dup-checker.store :as store]))

(defn all-filenames-by-digest [ ]
  (into {} (map (fn [ value ]
                  [(:md5_digest value) (:name value)])
                (query-all (sfm/db) (str "SELECT md5_digest, name"
                                         "  FROM file"
                                         " WHERE NOT excluded")))))

(defn catalog-filenames-by-digest [ catalog-id ]
  (into {} (map (fn [ value ]
                  [(:md5_digest value) (:name value)])
                (query-all (sfm/db)
                           [(str "SELECT md5_digest, name"
                                 "  FROM file"
                                 "  WHERE catalog_id=? AND NOT excluded")
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

(defn ensure-catalog [ catalog-name store-uri ]
  (let [catalog-type (.getScheme store-uri)
        root-path (.getSchemeSpecificPart store-uri)]
    (if-let [ existing-catalog-id (get-catalog-id catalog-name) ]
      (update-catalog-date existing-catalog-id)
      (create-catalog catalog-name root-path catalog-type))))

(defn- file-cataloged? [ catalog-id file-info ]
  (> (query-scalar (sfm/db)
                   [(str "SELECT COUNT(file_id)"
                         "  FROM file"
                         " WHERE file.name = ?"
                         "   AND file.catalog_id = ?")
                    (:name file-info)
                    catalog-id])
     0))

(defn- get-excluded-catalog-files [ catalog-id]
   (query-all (sfm/db)
              [(str "SELECT *"
                    "  FROM file"
                    " WHERE file.catalog_id = ?"
                    "   AND excluded"
                    " ORDER BY name")
               catalog-id]))

(defn- get-all-catalog-files
  ([ catalog-id file-pattern ]
   (query-all (sfm/db)
              (cond->
                  [(str "SELECT *"
                        "  FROM file"
                        " WHERE file.catalog_id = ?"
                        "   AND NOT excluded"
                        (when file-pattern
                          (str "  AND file.name like ?"))
                        " ORDER BY name")
                   catalog-id]
                file-pattern (conj file-pattern))))

  ([ catalog-id ]
   (get-all-catalog-files catalog-id nil)))

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
    (doseq [ f (store/get-store-files file-store) ]
      (catalog-file catalog-files catalog-id f))))

(defn- query-catalogs []
  (query-all (sfm/db)
             [(str "SELECT catalog.catalog_id, catalog.name, catalog.root_path, catalog.created_on,"
                   "       catalog.updated_on, count(file_id) as n, sum(CASE WHEN excluded THEN 1 ELSE 0 END) as excluded,"
                   "       sum(file.size) as size, catalog_type.catalog_type"
                   "  FROM catalog, catalog_type"
                   "  LEFT JOIN file ON catalog.catalog_id = file.catalog_id"
                   " WHERE catalog.catalog_type_id = catalog_type.catalog_type_id"
                   " GROUP BY catalog.catalog_id, catalog.name, catalog.updated_on, catalog_type, catalog.root_path"
                   " ORDER BY catalog.name")]))

(defn- cmd-catalog-list
  "List all catalogs"
  [ ]
  (table
   [:name :created_on :updated_on [:catalog_uri 50] :n :excluded :size ]
   (map #(assoc % :catalog_uri (str (:catalog_type %) ":" (:root_path %)))
    (query-catalogs))))

(defn- get-required-catalog-id [ catalog-name ]
  (or (get-catalog-id catalog-name)
      (fail "No known catalog: " catalog-name)))

(defn- cmd-catalog-file-duplicates
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
                           "     AND NOT excluded"
                           "    GROUP BY md5_digest)"
                           " WHERE count > 1"
                           " ORDER BY count")
                      catalog-id])))))

(defn- cmd-catalog-file-list
  "List all files present in a catalog."

  ([ catalog-name file-pattern ]
   (table
    [:md5_digest :size :name]
    (get-all-catalog-files (get-required-catalog-id catalog-name)
                           file-pattern)))

  ([ catalog-name ]
   (cmd-catalog-file-list catalog-name nil)))

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

(defn- file-path [ & segs ]
  (.getCanonicalPath (apply clojure.java.io/file segs)))

(defn- cmd-catalog-link
  "Extend the catalog with missing files from another catalog via links."

  [ catalog-name other-catalog-name ]
  (let [catalog-id (get-required-catalog-id catalog-name)
        other-catalog-id (get-required-catalog-id other-catalog-name)
        current-catalog-files (catalog-files-by-digest catalog-name)
        catalog-root (:root_path (get-catalog catalog-id))
        other-catalog-root (:root_path (get-catalog other-catalog-id))]

    (doseq [ file (remove #(get current-catalog-files (:md5_digest %))
                          (get-all-catalog-files other-catalog-id))]

      (let [target-path (.getParent (.toPath (java.io.File. (file-path catalog-root (:name file)))))
            link-target (.toPath (java.io.File. (file-path catalog-root (:name file))))
            link-source (.toPath (java.io.File. (file-path other-catalog-root (:name file))))]

        (log/info "Linking: " link-source)
        (java.nio.file.Files/createDirectories target-path (make-array java.nio.file.attribute.FileAttribute 0))
        (java.nio.file.Files/createLink link-target link-source)
        (jdbc/insert! (sfm/db)
                      :file
                      (-> file
                               (dissoc :file_id)
                               (assoc :catalog_id catalog-id)
                               (assoc :excluded false)))))))


(defn- cmd-catalog-file-missing
  "Identify files missing in a given catalog."

  [ catalog-name required-catalog-name ]

  (let [catalog-files (catalog-files-by-digest catalog-name) ]
    (table
     [:last_modified_on :md5_digest :size :name ]
     (remove #(get catalog-files (:md5_digest %))
             (get-all-catalog-files
              (get-required-catalog-id required-catalog-name))))))

(defn- cmd-catalog-create
  "Create a catalog rooted at a given URI."

  [ catalog-name store-uri ]
  (let [uri (java.net.URI. store-uri)]
    (catalog-files (ensure-catalog catalog-name uri) (store/get-store uri))))

(defn- cmd-catalog-update
  "Create a catalog rooted at a given URI."

  [ catalog-name ]
  (let [catalog-id (get-required-catalog-id catalog-name)
        {scheme :catalog_type
         scheme-specific-part :root_path} (get-catalog catalog-id)]
    (catalog-files catalog-id (store/get-store (java.net.URI. scheme scheme-specific-part nil)))))

(defn- cmd-catalog-exclude-extension
  "Exclude files from a catalog by their extension."

  [ catalog-name & extensions ]
  (let [catalog-id (get-required-catalog-id catalog-name)]
    (doseq [ ext extensions ]
      (jdbc/update! (sfm/db)
                    :file
                    {:excluded true}
                    ["catalog_id=? AND extension=?" catalog-id ext]))))

(defn- cmd-catalog-excluded-list
  "List excluded files in a catalog."

  [ catalog-name ]

  (table
   [:md5_digest :size :name]
   (get-excluded-catalog-files (get-required-catalog-id catalog-name))))

(defn- cmd-catalog-exclude-pattern
  "Exclude files from a catalog that match a specific fileame pattern."

  [ catalog-name file-pattern]
  (let [catalog-id (get-required-catalog-id catalog-name)]
    (jdbc/update! (sfm/db)
                  :file
                  {:excluded true}
                  ["catalog_id=? AND name LIKE ?" catalog-id file-pattern])))

(defn- cmd-catalog-exclude-catalog
  "Exclude files from a catalog that are already in another catalog."

  [ catalog-name & other-catalog-names ]
  (let [catalog-id (get-required-catalog-id catalog-name)
        other-catalog-ids (map get-required-catalog-id other-catalog-names)]
    (doseq [ other-catalog-id other-catalog-ids ]
      (jdbc/execute! (sfm/db)
                     [(str "UPDATE file"
                           "   SET excluded=true"
                           " WHERE catalog_id=?"
                           "   AND md5_digest in (SELECT md5_digest"
                           "                        FROM file"
                           "                       WHERE NOT excluded"
                           "                         AND catalog_id=?)")
                      catalog-id
                      other-catalog-id]))))

(defn- cmd-catalog-exclude-reset
  "Reset all catalog file exclusions."

  [ & catalog-names ]
  (let [catalog-ids (map get-required-catalog-id catalog-names)]
    (doseq [ catalog-id catalog-ids ]
      (jdbc/update! (sfm/db)
                    :file
                    {:excluded false}
                    ["catalog_id=?" catalog-id]))))


(def file-subcommands
  #^{:doc "Commands for operating on files within catalogs."}
  {"duplicates" #'cmd-catalog-file-duplicates
   "ls" #'cmd-catalog-file-list
   "missing" #'cmd-catalog-file-missing})

(def exclude-subcommands
  #^{:doc "Commands for marking files within a catalog as excluded."}
  {"catalog" #'cmd-catalog-exclude-catalog
   "extension" #'cmd-catalog-exclude-extension
   "ls" #'cmd-catalog-excluded-list
   "pattern" #'cmd-catalog-exclude-pattern
   "reset" #'cmd-catalog-exclude-reset})

(def subcommands
  #^{:doc "Catalog subcommands"}
  {"create" #'cmd-catalog-create
   "exclude" exclude-subcommands
   "export" #'cmd-catalog-export
   "file" file-subcommands
   "import" #'cmd-catalog-import
   "link" #'cmd-catalog-link
   "ls" #'cmd-catalog-list
   "rm" #'cmd-catalog-remove
   "set-root" #'cmd-catalog-set-root
   "update" #'cmd-catalog-update})
