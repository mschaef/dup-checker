(ns dup-checker.google-oauth
  (:use playbook.core
        sql-file.sql-util
        dup-checker.util)
  (:require [taoensso.timbre :as log]
            [clojure.java.browse :as browse]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.params :as params]
            [ring.util.response :as ring]
            [clojure.core.async :as async]
            [sql-file.middleware :as sfm]
            [clojure.java.jdbc :as jdbc]
            [dup-checker.http :as http]))

(defn- google-oauth-config []
  (:installed (try-parse-json (slurp "google-oauth.json"))))

(def token-expiry-margin-sec 300)

(defn- request-google-authorization [ oauth scope ]
  (browse/browse-url
   (format "%s?client_id=%s&redirect_uri=%s&response_type=%s&scope=%s"
           (:auth_uri oauth)
           (:client_id oauth)
           "http://localhost:8080"
           "code"
           scope)))

(defn- start-site [ handler ]
  (let [ http-port 8080 ]
    (log/info "Starting Webserver on port" http-port)
    (let [ server (jetty/run-jetty (-> handler
                                       params/wrap-params)
                                   { :port http-port :join? false })]
      (add-shutdown-hook #(.stop server))
      server)))

(defn- google-authenticate [ oauth scope ]
  (let [c (async/chan)
        server (start-site #(let [ params (:params %) ]
                              (async/>!! c (or (params "code") false))
                              (if-let [ error (params "error") ]
                                (ring/response (str "Error: " error))
                                (ring/response "OK"))))]
    (request-google-authorization oauth scope)
    (let [ resp (async/<!! c)]
      (async/thread
        (Thread/sleep 1000)
        (.stop server))
      resp)))

(defn- google-exchange-code-for-jwt [ oauth code ]
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

(defn- google-request-access-token [ oauth refresh-token ]
  (log/info "Requesting access token")
  (http/post-json
   (format "%s?client_id=%s&client_secret=%s&refresh_token=%s&grant_type=refresh_token"
           (:token_uri oauth)
           (:client_id oauth)
           (:client_secret oauth)
           refresh-token)))

(defn- google-ensure-creds []
  (let [oauth (google-oauth-config)]
    (assoc oauth :refresh-token (or (load-google-refresh-token)
                                    (fail "Not authenticated to Google")))))

(defn- google-ensure-access-token [ creds ]
  (if (or (not (:expires-on creds))
          (.isAfter
           (.plusSeconds (java.time.LocalDateTime/now) token-expiry-margin-sec)
           (:expires-on creds)))
    (let [access-token (google-request-access-token creds (:refresh-token creds))]
      (log/info "Access token acquired successfully")
      (-> creds
          (merge access-token)
          (assoc :expires-on (.plusSeconds (java.time.LocalDateTime/now) (:expires_in access-token)))))
    creds))

(defn google-auth-provider [ ]
  (let [ provider-fn (let [ google-creds (atom (google-ensure-creds)) ]
                       (fn []
                         (swap! google-creds google-ensure-access-token)
                         (:access_token @google-creds)))]
    (provider-fn)
    provider-fn))

(defn google-login [scope]
  (let [oauth (google-oauth-config)]
    (or (load-google-refresh-token)
        (if-let [ authorization-code (google-authenticate oauth scope) ]
          (if-let [ jwt (google-exchange-code-for-jwt oauth authorization-code) ]
            (store-google-token jwt)
            (fail "Cannot acquire Google JWT from authorization code."))
          (fail "Google authentication failed")))))

