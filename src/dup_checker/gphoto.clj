(ns dup-checker.gphoto
  (:use playbook.core
        sql-file.sql-util
        dup-checker.util)
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [clojure.java.browse :as browse]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.params :as params]
            [ring.util.response :as ring]
            [clojure.core.async :as async]
            [sql-file.middleware :as sfm]
            [clojure.java.jdbc :as jdbc]
            [dup-checker.http :as http]
            [dup-checker.catalog :as catalog]))

(def token-expiry-margin-sec 300)

(defn- request-google-authorization [ oauth ]
  (browse/browse-url
   (format "%s?client_id=%s&redirect_uri=%s&response_type=%s&scope=%s"
           (:auth_uri oauth)
           (:client_id oauth)
           "http://localhost:8080"
           "code"
           "https://www.googleapis.com/auth/photoslibrary.readonly")))

(defn- start-site [ handler ]
  (let [ http-port 8080 ]
    (log/info "Starting Webserver on port" http-port)
    (let [ server (jetty/run-jetty (-> handler
                                       params/wrap-params)
                                   { :port http-port :join? false })]
      (add-shutdown-hook #(.stop server))
      server)))

(defn- gphoto-authenticate [ oauth ]
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

(defn- gphoto-exchange-code-for-jwt [ oauth code ]
  (http/post-json
   (format "%s?client_id=%s&client_secret=%s&redirect_uri=%s&code=%s&grant_type=%s"
           "https://oauth2.googleapis.com/token"
           (:client_id oauth)
           (:client_secret oauth)
           "http://localhost:8080"
           code
           "authorization_code")))

(defn- load-google-refresh-token [ ]
  (query-scalar (sfm/db)
                [(str "SELECT refresh_token FROM google_jwt")]))

(defn- store-google-token [ jwt ]
  (jdbc/execute! (sfm/db)
                 [(str "MERGE INTO google_jwt "
                       "  USING (VALUES (?)) new_jwt (refresh_token)"
                       "  ON true "
                       "  WHEN MATCHED THEN UPDATE SET google_jwt.refresh_token=new_jwt.refresh_token"
                       "  WHEN NOT MATCHED THEN INSERT (refresh_token) VALUES (new_jwt.refresh_token) ")
                  (:refresh_token jwt)]))

(defn- cmd-gphoto-logout []
  (jdbc/delete! (sfm/db) :google_jwt []))

(defn- gphoto-oauth-config []
  (:installed (try-parse-json (slurp "google-oauth.json"))))

(defn- cmd-gphoto-login []
  (let [oauth (gphoto-oauth-config)]
    (or (load-google-refresh-token)
        (if-let [ authorization-code (gphoto-authenticate oauth) ]
          (if-let [ jwt (gphoto-exchange-code-for-jwt oauth authorization-code) ]
            (store-google-token jwt)
            (fail "Cannot acquire Google JWT from authorization code."))
          (fail "Google authentication failed")))))

(defn- gphoto-request-access-token [ oauth refresh-token ]
  (log/info "Requesting access token")
  (http/post-json
   (format "%s?client_id=%s&client_secret=%s&refresh_token=%s&grant_type=refresh_token"
           (:token_uri oauth)
           (:client_id oauth)
           (:client_secret oauth)
           refresh-token)))

(defn- gphoto-ensure-creds []
  (let [oauth (gphoto-oauth-config)]
    (assoc oauth :refresh-token (or (load-google-refresh-token)
                                    (fail "Not authenticated to Google")))))

(defn- gphoto-ensure-access-token [ creds ]
  (if (or (not (:expires-on creds))
          (.isAfter
           (.plusSeconds (java.time.LocalDateTime/now) token-expiry-margin-sec)
           (:expires-on creds)))
    (let [access-token (gphoto-request-access-token creds (:refresh-token creds))]
      (log/info "Access token acquired successfully")
      (-> creds
          (merge access-token)
          (assoc :expires-on (.plusSeconds (java.time.LocalDateTime/now) (:expires_in access-token)))))
    creds))

(defn- gphoto-auth-provider [ ]
  (let [ provider-fn (let [ gphoto-creds (atom (gphoto-ensure-creds)) ]
                       (fn []
                         (swap! gphoto-creds gphoto-ensure-access-token)
                         (:access_token @gphoto-creds)))]
    (provider-fn)
    provider-fn))

(defn- cmd-gphoto-api-token []
  (pprint/pprint (gphoto-ensure-access-token (gphoto-ensure-creds))))


(defn- get-gphoto-paged-stream [ gphoto-auth url items-key page-size ]
  (letfn [(query-page [ page-token ]
            (let [ response (with-retries
                              (http/get-json (str url
                                                  (str "?pageSize=" page-size)
                                                  (when page-token
                                                    (str "&pageToken=" page-token)))
                                             :auth gphoto-auth))]
              (if-let [ next-page-token (:nextPageToken response)]
                (lazy-seq (concat (items-key response)
                                  (query-page next-page-token)))
                (items-key response))))]
    (query-page nil)))

(defn- get-gphoto-albums [ gphoto-auth ]
  (get-gphoto-paged-stream gphoto-auth "https://photoslibrary.googleapis.com/v1/albums" :albums 50))

(defn- get-gphoto-media-items [ gphoto-auth ]
  (get-gphoto-paged-stream gphoto-auth "https://photoslibrary.googleapis.com/v1/mediaItems" :mediaItems 100))

(defn- gphoto-info [ gphoto-auth p ]
  {:full-path (:id p)
   :extension (get-file-extension (java.io.File. (:filename p)))
   :last-modified-on (java.time.Instant/parse (get-in  p [:mediaMetadata :creationTime]))
   :name (:filename p)
   :size -1
   :data-stream-fn #(http/get-json (:baseUrl p)
                                   :auth gphoto-auth
                                   :as-binary-stream true)})

(defn- cmd-list-gphoto-albums
  "List available Google Photo Albums"

  []
  (let [ gphoto-auth (gphoto-auth-provider) ]
    (doseq [ album (get-gphoto-albums gphoto-auth) ]
      (pprint/pprint album))))

(defn- cmd-list-gphoto-media-items
  "List available Google Photo media items."

  []
  (let [ gphoto-auth (gphoto-auth-provider) ]
    (doseq [ item (get-gphoto-media-items gphoto-auth) ]
      (pprint/pprint item))))

(defn get-snapshot-item-ids [ ]
  (set (map :gphoto_id (query-all (sfm/db)
                                  "SELECT gphoto_id FROM gphoto_media_item"))))

(defn- snapshot-item [ existing-item-ids media-item ]
  (cond
    (existing-item-ids (:id media-item))
    (log/info "Item already in snapshot:" (:id media-item))

    :else
    (do
      (log/info "Adding item to snapshot:" (:id media-item))
      (jdbc/insert! (sfm/db)
                    :gphoto_media_item
                    {:gphoto_id (:id media-item)
                     :name (:filename media-item)
                     :extension (.toLowerCase (get-filename-extension (:filename media-item)))
                     :mime_type (:mimeType media-item)
                     :creation_time (java.time.Instant/parse (get-in media-item [:mediaMetadata :creationTime]))
                     :media_metadata (pr-str (:mediaMetadata media-item))}))))

(defn- cmd-gphoto-snapshot-update
  "Update the current gphoto snapshot."

  [ ]

  (let [existing-item-ids (get-snapshot-item-ids)
        media-items (get-gphoto-media-items (gphoto-auth-provider))]
    (doseq [ media-item media-items ]
      (snapshot-item existing-item-ids media-item))))

(defn- get-snapshot-media-items []
  (query-all (sfm/db)
             [(str "SELECT entry_id, gphoto_id, name, creation_time"
                   "  FROM gphoto_media_item"
                   " ORDER BY creation_time")]))

(defn- cmd-gphoto-snapshot-list
  "List the current gphoto snapshot."

  [ ]

  (table
   (map (fn [ media-item ]
          {:entry-id (:entry_id media-item)
           :gphoto-id (:gphoto_id media-item)
           :name (:name media-item)
           :creation-time (:creation_time media-item)})
        (get-snapshot-media-items))))

(def path-sep java.io.File/separator)

(def df (java.text.SimpleDateFormat. (format "yyyy%sMM%sdd" path-sep path-sep)))

(defn- mkdir-if-needed [ path ]
  (assert path)
  (let [ f (java.io.File. path) ]
    (when (not (.exists f))
      (log/info "Creating target directory: " path)
      (.mkdirs f))))

(defn- backup-media-item [ gphoto-auth media-item ]
  (let [target-file (:target-filename media-item)
        base-url (:base_url media-item)]
    (with-exception-barrier (str "Downloading image data: " (:gphoto_id media-item))
      (log/info "Backing up" target-file)
      (assert (and target-file base-url))
      (let [ f (java.io.File. target-file) ]
        (mkdir-if-needed (:target-path media-item))
        (with-retries
          (with-open [ in (http/get-json base-url :auth gphoto-auth :as-binary-stream true) ]
            (clojure.java.io/copy in f)))))))

(defn- add-media-item-local-file-info [ base-path media-item ]
  (let [target-path (str base-path path-sep (.format df (:creation_time media-item)))
        target-filename (str target-path path-sep (:name media-item))]
    (merge media-item
           {:target-path target-path
            :target-filename target-filename
            :local-file-exists? (.exists (java.io.File. target-filename))})))

(defn- batch-get-media-items [ gphoto-auth item-ids ]
  (log/info "Fetching media info for" (count item-ids) "item(s).")
  (let [ response (http/get-json (str "https://photoslibrary.googleapis.com/v1/mediaItems:batchGet?"
                                      (clojure.string/join "&" (map #(str "mediaItemIds=" %) (set item-ids))))
                                 :auth gphoto-auth)]
    (into {}
          (map (fn [ media-item ]
                 [(:id media-item) media-item])
               (map :mediaItem (:mediaItemResults response))))))


(defn add-media-item-base-url [ media-info media-item ]
  (assoc media-item
         :base_url (get-in media-info [ (:gphoto_id media-item) :baseUrl ])))

(defn- backup-batch [ gphoto-auth media-items ]
  (let [current-media-info (batch-get-media-items gphoto-auth (map :gphoto_id media-items))
        media-items (map (partial add-media-item-base-url current-media-info)
                         media-items)]
    (doseq [ media-item media-items ]
      (backup-media-item gphoto-auth media-item))))

(defn- cmd-gphoto-snapshot-backup
  "Backup the current gphoto snapshot to a local filesystem directory."

  [ base-path ]

  (let [gphoto-auth (gphoto-auth-provider)
        all-media-items (map (partial add-media-item-local-file-info base-path)
                             (get-snapshot-media-items))
        present-media-items (filter :local-file-exists? all-media-items)
        missing-media-items (remove :local-file-exists? all-media-items)]
    (log/info "Backing up" (count missing-media-items) "item(s) of"
              (count all-media-items) "in snapshot. (" (count present-media-items)
              "already present locally.)")
    (doseq [ media-items (partition-all 50 missing-media-items)]
      (backup-batch gphoto-auth media-items))))

(defn- cmd-gphoto-catalog
  "Catalog the contents of the gphoto album."

  [ catalog-name ]

  (let [ gphoto-auth (gphoto-auth-provider) ]
    (catalog/catalog-files
     (catalog/ensure-catalog catalog-name "gphoto" "gphoto")
     (map (partial gphoto-info gphoto-auth) (get-gphoto-media-items gphoto-auth)))))

(def gphoto-snapshot-subcommands
  #^{:doc "Snapshot subcommands"}
  {"update" #'cmd-gphoto-snapshot-update
   "ls" #'cmd-gphoto-snapshot-list
   "backup" #'cmd-gphoto-snapshot-backup})

(def subcommands
  #^{:doc "Google Photo subcommands"}
  {"login" #'cmd-gphoto-login
   "logout" #'cmd-gphoto-logout
   "api-token" #'cmd-gphoto-api-token
   "lsa" #'cmd-list-gphoto-albums
   "lsmi" #'cmd-list-gphoto-media-items
   "snapshot" gphoto-snapshot-subcommands
   "catalog" #'cmd-gphoto-catalog})
