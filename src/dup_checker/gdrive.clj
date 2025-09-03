(ns dup-checker.gdrive
  (:use playbook.core
        sql-file.sql-util
        dup-checker.util)
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [sql-file.middleware :as sfm]
            [clojure.java.jdbc :as jdbc]
            [dup-checker.http :as http]
            [dup-checker.google-oauth :as google-oauth]))


;;;; Commands

(defn cmd-gdrive-login
  "Login to a Google account."

  []
  (google-oauth/google-login ["https://www.googleapis.com/auth/drive"]))

(defn cmd-gdrive-logout
  "Log out from any currently authenticated Google account."

  []
  (jdbc/delete! (sfm/db) :google_jwt []))

(defn cmd-gdrive-api-token
  "Return an API token for the currently authenticated Google account."

  []
  (pprint/pprint ((google-oauth/google-auth-provider))))


(defn- get-gdrive-paged-stream [ gdrive-auth url items-key page-size ]
  (letfn [(query-page [ page-token ]
            (let [ response (with-retries
                              (http/get-json (str url
                                                  (str "?pageSize=" page-size)
                                                  (when page-token
                                                    (str "&pageToken=" page-token)))
                                             :auth gdrive-auth))]
              (if-let [ next-page-token (:nextPageToken response)]
                (lazy-seq (concat (items-key response)
                                  (query-page next-page-token)))
                (items-key response))))]
    (query-page nil)))

(defn- get-gdrive-files [ gdrive-auth ]
  (get-gdrive-paged-stream gdrive-auth "https://www.googleapis.com/drive/v3/files" :files 100))

(defn cmd-gdrive-list-files
  "List available Google Drive files."

  []
  (let [gdrive-auth (google-oauth/google-auth-provider)
        files (get-gdrive-files gdrive-auth)]
    (table [[:kind 15]
            [:mimeType 40]
            :name]
           files)))

(defn cmd-gdrive-list-files-raw
  "List available Google Drive files with all available information."

  []
  (let [gdrive-auth (google-oauth/google-auth-provider)
        files (get-gdrive-files gdrive-auth)]
    (doseq [f files]
      (pprint/pprint f))))

(def subcommands
  #^{:doc "Commands for interacting with a Google Drive."}
  {"login" #'cmd-gdrive-login
   "logout" #'cmd-gdrive-logout
   "api-token" #'cmd-gdrive-api-token
   "ls" #'cmd-gdrive-list-files
   "lsr" #'cmd-gdrive-list-files-raw})
