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

(defn- store-google-token [ jwt ]
  (jdbc/execute! (sfm/db)
                 [(str "MERGE INTO google_jwt "
                       "  USING (VALUES (?)) new_jwt (refresh_token)"
                       "  ON true "
                       "  WHEN MATCHED THEN UPDATE SET google_jwt.refresh_token=new_jwt.refresh_token"
                       "  WHEN NOT MATCHED THEN INSERT (refresh_token) VALUES (new_jwt.refresh_token) ")
                  (:refresh_token jwt)]))

(defn cmd-gphoto-logout []
  (jdbc/delete! (sfm/db) :google_jwt []))

(defn- gphoto-oauth-config []
  (:installed (try-parse-json (slurp "google-oauth.json"))))

(defn cmd-gphoto-login []
  (let [oauth (gphoto-oauth-config)]
    (or (load-google-refresh-token)
        (if-let [ authorization-code (gphoto-authenticate oauth) ]
          (if-let [ jwt (gphoto-exchange-code-for-jwt oauth authorization-code) ]
            (store-google-token jwt)
            (fail "Cannot acquire Google JWT from authorization code."))
          (fail "Google authentication failed")))))

(defn- gphoto-request-access-token [ oauth refresh-token ]
  (log/info "Requesting access token")
  (http-post-json
   (format "%s?client_id=%s&client_secret=%s&refresh_token=%s&grant_type=refresh_token"
           (:token_uri oauth)
           (:client_id oauth)
           (:client_secret oauth)
           refresh-token)))

(defn- gphoto-ensure-creds []
  (let [oauth (gphoto-oauth-config)]
    (assoc oauth :refresh-token (or (load-google-refresh-token)
                                    (fail "Not authenticated to Google")))))

(def hysterisis 60)

(defn- gphoto-ensure-access-token [ creds ]
  (if (or (not (:expires-on creds))
          (.isAfter
           (.plusSeconds (java.time.LocalDateTime/now) hysterisis)
           (:expires-on creds)))
    (let [access-token (gphoto-request-access-token creds (:refresh-token creds))]
      (-> creds
          (merge access-token)
          (assoc :expires-on (.plusSeconds (java.time.LocalDateTime/now) (:expires_in access-token)))))
    creds))

(defn- gphoto-auth-provider [ ]
  (let [ gphoto-creds (atom (gphoto-ensure-creds)) ]
    (fn []
      (swap! gphoto-creds gphoto-ensure-access-token)
      (:access_token @gphoto-creds))))

(defn- cmd-gphoto-api-token []
  (pprint/pprint (gphoto-ensure-access-token (gphoto-ensure-creds))))

(defn- get-gphoto-paged-stream [ url items-key ]
  (let [ gphoto-auth (gphoto-auth-provider)]
    (letfn [(query-page [ page-token ]
              (let [ response (http-get-json (str url (when page-token
                                                        (str "?pageToken=" page-token)))
                                             gphoto-auth)]
                (if-let [ next-page-token (:nextPageToken response)]
                  (lazy-seq (concat (items-key response)
                                    (query-page next-page-token)))
                  (items-key response))))]
      (query-page nil))))

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
