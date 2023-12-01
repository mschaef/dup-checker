(ns dup-checker.gphoto
  (:use playbook.core
        sql-file.sql-util
        dup-checker.util)
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [clj-http.client :as http]
            [clojure.java.browse :as browse]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.params :as params]
            [ring.util.response :as ring]
            [clojure.core.async :as async]
            [sql-file.middleware :as sfm]
            [clojure.java.jdbc :as jdbc]))

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
  (http-post-json
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

(defn- store-google-refresh-token [ refresh-token  ]
  (jdbc/execute! (sfm/db)
                 [(str "MERGE INTO google_jwt "
                       "  USING (VALUES (?)) new_jwt (refresh_token)"
                       "  ON true "
                       "  WHEN MATCHED THEN UPDATE SET google_jwt.refresh_token=new_jwt.refresh_token"
                       "  WHEN NOT MATCHED THEN INSERT (refresh_token) VALUES (new_jwt.refresh_token) ")
                  refresh-token])
  refresh-token)

(defn cmd-gphoto-logout []
  (jdbc/delete! (sfm/db) :google_jwt []))

(defn cmd-gphoto-login []
  (let [oauth (:installed (try-parse-json (slurp "google-oauth.json")))]
    (or (load-google-refresh-token)
        (if-let [ authorization-code (gphoto-authenticate oauth) ]
          (if-let [ jwt (gphoto-exchange-code-for-jwt oauth authorization-code) ]
            (store-google-refresh-token (:access_token jwt))
            (fail "Cannot acquire Google JWT from authorization code."))
          (fail "Google authentication failed")))))

(defn- gphoto-refresh-token [ oauth refresh-token ]
  (http-post-json
   (format "%s?client_id=%s&client_secret=%s&refresh_token=%s&grant_type=refresh_token"
           (:token_uri oauth)
           (:client_id oauth)
           (:client_secret oauth)
           refresh-token)))

(defn- cmd-gphoto-api-token []
  (let [oauth (:installed (try-parse-json (slurp "google-oauth.json")))]
    (log/spy :info (gphoto-refresh-token oauth (or (load-google-refresh-token)
                                                   (fail "Not authenticated to Google"))))))

(defn- get-gphoto-paged-stream [ url items-key ]
  (letfn [(query-page [ page-token ]
            (let [ response (http-get-json (str url (when page-token
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

(def subcommands
  #^{:doc "Google Photo subcommands"}
  {"login" #'cmd-gphoto-login
   "logout" #'cmd-gphoto-logout
   "api-token" #'cmd-gphoto-api-token
   "lsa" #'cmd-list-gphoto-albums
   "lsmi" #'cmd-list-gphoto-media-items})
