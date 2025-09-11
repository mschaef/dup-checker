(ns dup-checker.gdrive
  (:use playbook.core
        sql-file.sql-util
        dup-checker.util)
  (:require [playbook.config :as config]
            [clojure.pprint :as pprint]
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


(defn- get-gdrive-paged-seq
  ([ gdrive-auth url items-key query-params ]
   (letfn [(query-page [ page-token ]
             (let [ response (with-retries
                               (http/get-json (encode-url url (merge
                                                               {:pageSize 100}
                                                               query-params
                                                               (if page-token
                                                                 {:pageToken page-token}
                                                                 {})))
                                              :auth gdrive-auth))]
               (if-let [ next-page-token (:nextPageToken response)]
                 (lazy-seq (concat (items-key response)
                                   (query-page next-page-token)))
                 (items-key response))))]
     (query-page nil)))

  ([ gdrive-auth url items-key ]
   (get-gdrive-paged-seq gdrive-auth url items-key {})))


(defn- get-gdrive-stream
  ([ gdrive-auth url query-params ]
   (http/get-binary-stream (encode-url url query-params)
                           :auth gdrive-auth))

  ([ gdrive-auth url ]
   (get-gdrive-stream gdrive-auth url {})))


(defn- get-gdrive-files
  ([ gdrive-auth ]
   (get-gdrive-paged-seq gdrive-auth "https://www.googleapis.com/drive/v3/files"
                         :files
                         {:pageSize 1000
                          :fields "nextPageToken,files/kind,files/id,files/name,kind,files/mimeType,files/parents,files/quotaBytesUsed"
                          })))

(defn- query-gdrive-files
  ([ gdrive-auth query ]
   (get-gdrive-paged-seq gdrive-auth "https://www.googleapis.com/drive/v3/files"
                            :files
                            {:pageSize 1000
                             :q query
                             :fields "nextPageToken,files/kind,files/id,files/name,kind,files/mimeType,files/parents,files/quotaBytesUsed"})))

(defn- get-gdrive-takeout-files [ gdrive-auth ]
  (when-let [takeout-folder (first (query-gdrive-files gdrive-auth "name='Takeout' and mimeType='application/vnd.google-apps.folder'"))]
    (query-gdrive-files gdrive-auth (str "'" (:id takeout-folder) "' in parents"))))

(defn- takeout-filename-timestamp [ filename ]
  (and (.startsWith filename "takeout-")
       (.substring filename 8 24)))

(defn- get-gdrive-takeout-downloads [ gdrive-auth ]
  (set (map takeout-filename-timestamp
                        (map :name (get-gdrive-takeout-files gdrive-auth)))))

(defn- file-table [files]
  (table [[:mimeType 40]
          [:id 35]
          [:name 48]
          [:quotaBytesUsed 16]
          :parents]
         files))

(defn cmd-gdrive-list-files
  "List available Google Drive files."

  []
  (let [gdrive-auth (google-oauth/google-auth-provider)
        files (get-gdrive-files gdrive-auth)]
    (file-table files)))

(defn cmd-gdrive-list-takeout-files
  "List available Google Drive files produced by Google Takeout."

  []
  (let [gdrive-auth (google-oauth/google-auth-provider)
        files (get-gdrive-takeout-files gdrive-auth)]
    (file-table files)))

(defn cmd-gdrive-list-takeout-downloads
  "List available Google Drive downloads produced by Google Takeout."

  []
  (let [gdrive-auth (google-oauth/google-auth-provider)
        downloads (get-gdrive-takeout-downloads gdrive-auth)]
    (doseq [d downloads]
      (println d))))

(defn cmd-gdrive-list-files-raw
  "List available Google Drive files with all available information."

  []
  (let [gdrive-auth (google-oauth/google-auth-provider)
        files (get-gdrive-files gdrive-auth)]
    (doseq [f files]
      (pprint/pprint f))))

(defn- get-gdrive-takeout-files [ gdrive-auth ]
  (when-let [takeout-folder (first (query-gdrive-files gdrive-auth "name='Takeout' and mimeType='application/vnd.google-apps.folder'"))]
    (query-gdrive-files gdrive-auth (str "'" (:id takeout-folder) "' in parents"))))

(defn cmd-gdrive-list-takeout-download-files
  [download-name]

  (let [gdrive-auth (google-oauth/google-auth-provider)
        files (get-gdrive-takeout-files gdrive-auth)]
    (file-table (sort-by :name (filter #(.startsWith (:name %) (str "takeout-" download-name)) files)))))

(defn cmd-gdrive-get-file
  "Get a Google Drive file by ID"

  [file-id output-filename]

  (let [gdrive-auth (google-oauth/google-auth-provider)
        buf (byte-array (config/cval :transfer-buffer-size))]

    (with-open [gdrive-stream (get-gdrive-stream gdrive-auth (str "https://www.googleapis.com/drive/v3/files/" file-id)
                                                 {:alt "media"})]
      (with-open [file-stream (clojure.java.io/output-stream output-filename)]
        (loop [tot-bytes 0]
          (let [bytes-read (.read gdrive-stream buf)]
            (when (pos? bytes-read)
              (.write file-stream buf 0 bytes-read)
              (recur (+ tot-bytes bytes-read)))))))))

(def subcommands
  #^{:doc "Commands for interacting with a Google Drive."}
  {"login" #'cmd-gdrive-login
   "logout" #'cmd-gdrive-logout
   "api-token" #'cmd-gdrive-api-token
   "ls" #'cmd-gdrive-list-files
   "lsr" #'cmd-gdrive-list-files-raw
   "lst" #'cmd-gdrive-list-takeout-files
   "lstd" #'cmd-gdrive-list-takeout-downloads
   "lstdf" #'cmd-gdrive-list-takeout-download-files
   "get" #'cmd-gdrive-get-file
   })
