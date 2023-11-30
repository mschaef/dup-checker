(ns dup-checker.core
  (:gen-class :main true)
  (:use playbook.core
        sql-file.sql-util)
  (:require [clojure.pprint :as pprint]
            [clj-commons.digest :as digest]
            [sql-file.core :as sql-file]
            [sql-file.middleware :as sfm]
            [playbook.logging :as logging]
            [playbook.config :as config]
            [taoensso.timbre :as log]
            [clojure.java.jdbc :as jdbc]
            [clj-http.client :as http]
            [ring.adapter.jetty :as jetty]
            [ring.util.response :as ring]
            [again.core :as again]
            [clojure.core.async :as async]
            [clojure.java.browse :as browse]
            [ring.middleware.params :as params]))

(defn- fail [ message & args ]
  (let [ full-message (apply str message args)]
    (println (str "Error: " full-message))
    (throw (RuntimeException. full-message))))

(defn- table
  ([ rows ]
   (table (keys (first rows)) rows))

  ([ ks rows ]
   (pprint/print-table ks rows)
   (println "n=" (count rows))))

(defn- http-request-json [ url ]
  (let [response (http/get url
                           {:headers
                            {:Authorization (str "Bearer " (System/getenv "GPHOTO_TOKEN"))}})]
    (and (= 200 (:status response))
         (try-parse-json (:body response)))))

(defn- http-request-json-post [ url ]
  (let [response (http/post url)]
    (and (= 200 (:status response))
         (try-parse-json (:body response)))))

(defn- s3-client []
  (-> (software.amazon.awssdk.services.s3.S3Client/builder)
      (.region software.amazon.awssdk.regions.Region/US_EAST_1)
      (.build)))

(defn- request-google-authorization [ oauth ]
  (browse/browse-url
   (format "%s?client_id=%s&redirect_uri=%s&response_type=%s&scope=%s"
           (:auth_uri oauth)
           (:client_id oauth)
           "http://localhost:8080"
           "code"
           "https://www.googleapis.com/auth/photoslibrary.readonly")))

(defn start-site [ handler ]
  (let [ http-port 8080 ]
    (log/info "Starting Webserver on port" http-port)
    (let [ server (jetty/run-jetty (-> handler
                                       params/wrap-params)
                                   { :port http-port :join? false })]
      (add-shutdown-hook #(.stop server))
      server)))

(defn- accept-oauth-code [ oauth ]
  (let [c (async/chan)
        server (start-site #(let [ params (:params %) ]
                              (async/>!! c (or (params "code") false))
                              (if-let [ error (params "error") ]
                                (ring/response (str "Error: " error))
                                (ring/response "OK"))))]
    (request-google-authorization oauth)
    (let [ resp (async/<!! c)]
      (.stop server)
      resp)))

(defn- exchange-code [ oauth code ]
  (http-request-json-post
   (format "%s?client_id=%s&client_secret=%s&redirect_uri=%s&code=%s&grant_type=%s"
           "https://oauth2.googleapis.com/token"
           (:client_id oauth)
           (:client_secret oauth)
           "http://localhost:8080"
           code
           "authorization_code")))

(defn gphoto-auth []
  (let [oauth (:installed (try-parse-json (slurp "google-oauth.json")))
        code (accept-oauth-code oauth)]
    (if code
      (log/spy :info (exchange-code oauth code)))))

(defn- get-gphoto-paged-stream [ url items-key ]
  (letfn [(query-page [ page-token ]
            (let [ response (http-request-json (str url (when page-token
                                                          (str "?pageToken=" page-token))))]
              (if-let [ next-page-token (:nextPageToken response)]
                (lazy-seq (concat (items-key response)
                                  (query-page next-page-token)))
                (items-key response))))]
    (query-page nil)))

(defn- get-gphoto-albums [ ]
  (get-gphoto-paged-stream "https://photoslibrary.googleapis.com/v1/albums" :albums))

(defn- get-gphoto-media-items [ ]
  (get-gphoto-paged-stream "https://photoslibrary.googleapis.com/v1/mediaItems" :mediaItems))

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
  (if (:md5-digest file-info)
    file-info
    (merge file-info
         ;;; MD5 only, because that's all the S3 API supports.
           {:md5-digest (again/with-retries retry-policy
                        ;;; Retry to accomodate potential I/O errors.
                          (digest/md5 (:file file-info)))})))

(defn- file-cataloged? [ catalog-id file-info ]
  (> (query-scalar (sfm/db)
                   [(str "SELECT COUNT(file_id)"
                         "  FROM file"
                         " WHERE file.name = ?"
                         "  AND file.catalog_id = ?")
                    (:name file-info)
                    catalog-id])
     0))

(defn- get-catalog-files [ catalog-id ]
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
    "mp3" "pdf" "arw" "ithmb" "gif" "nef" "png" "jpg"})

(defn- catalog-file [ catalog-files catalog-id file-info ]
  (cond
    (catalog-files (:name file-info))
    (log/info "File already cataloged:" (:name file-info))

    (not (image-extensions (.toLowerCase (:extension file-info))))
    (log/info "Skipping non-image file:" (:name file-info))

    :else
    (let [ file-info (compute-file-digests file-info )]
      (log/info "Adding file to catalog:" (:name file-info))
      (jdbc/insert! (sfm/db)
                    :file
                    {:name (:name file-info)
                     :catalog_id catalog-id
                     :extension (.toLowerCase (:extension file-info))
                     :size (:size file-info)
                     :last_modified_on (:last-modified-on file-info)
                     :md5_digest (:md5-digest file-info)}))))

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

(defn- current-hostname []
  (.getCanonicalHostName (java.net.InetAddress/getLocalHost)))

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

(defn- ensure-catalog [ catalog-name root-path catalog-type ]
  (if-let [ existing-catalog-id (get-catalog-id catalog-name ) ]
    (update-catalog-date existing-catalog-id)
    (create-catalog catalog-name root-path catalog-type)))

(defn- cmd-catalog-fs-files
  "Catalog the contents of an s3 bucket."
  [ root-path catalog-name ]
  (let [catalog-id (ensure-catalog catalog-name root-path "fs")
        root (clojure.java.io/file root-path)
        catalog-files (get-catalog-files catalog-id)]
    (doseq [f (filter #(.isFile %) (file-seq root))]
      (catalog-file catalog-files catalog-id (file-info root f)))))

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

(defn- s3-list-bucket-paged [ s3 bucket-name ]
  (letfn [(s3-list-objects [ cont-token ]
            (let [ resp (.listObjectsV2 s3 (-> (software.amazon.awssdk.services.s3.model.ListObjectsV2Request/builder)
                                               (.bucket bucket-name)
                                               (.continuationToken cont-token)
                                               (.build)))]
              (if (.isTruncated resp)
                (lazy-seq (concat (.contents resp)
                                  (s3-list-objects (.nextContinuationToken resp))))
                (.contents resp))))]
    (s3-list-objects nil)))

(defn- s3-blob-info [ f ]
  {:full-path (.key f)
   :extension (get-file-extension (java.io.File. (.key f)))
   :last-modified-on (.lastModified f)
   :name (.key f)
   :size (.size f)
   :md5-digest (.replace (.eTag f) "\"" "")})

(defn- cmd-catalog-s3-files
  "Catalog the contents of an s3 bucket."
  [ bucket-name catalog-name ]
  (let [catalog-id (ensure-catalog catalog-name bucket-name "s3")
        catalog-files (get-catalog-files catalog-id)]
    (doseq [f (s3-list-bucket-paged (s3-client) bucket-name)]
      (catalog-file catalog-files catalog-id (s3-blob-info f)))))

(defn- cmd-list-s3-bucket
  "List the contents of an s3 bucket."
  [ bucket-name ]
  (doseq [ bucket (s3-list-bucket-paged (s3-client) bucket-name)]
    (pprint/pprint bucket)))

(defn- cmd-list-gphoto-albums
  "List available Google Photo Albums"

  []
  (doseq [ album (get-gphoto-albums) ]
    (pprint/pprint album)))

(defn- cmd-list-gphoto-media-items
  "List available Google Photo media items."

  []
  (doseq [ item (get-gphoto-media-items) ]
    (pprint/pprint item)))


(def gphoto-subcommands
  #^{:doc "Google Photo subcommands"}
  {"lsa" #'cmd-list-gphoto-albums
   "lsmi" #'cmd-list-gphoto-media-items})

(def catalog-subcommands
  #^{:doc "Catalog subcommands"}
  {"ls" #'cmd-list-catalogs
   "list-files" #'cmd-list-catalog-files
   "rm" #'cmd-remove-catalog})

(def s3-subcommands
  #^{:doc "AWS S3 subcommands"}
  {"ls" #'cmd-list-s3-bucket
   "catalog" #'cmd-catalog-s3-files})

(def subcommands
  {"s3" s3-subcommands
   "catalog" catalog-subcommands
   "gphoto" gphoto-subcommands
   "fscat" #'cmd-catalog-fs-files
   "describe" #'cmd-describe-file
   "list-dups" #'cmd-list-dups
   "gphoto-auth" #'gphoto-auth})

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
   :schemas [[ "dup-checker" 1 ]]})


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
