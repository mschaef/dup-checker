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
                             :fields "nextPageToken,files/kind,files/id,files/name,kind,files/mimeType,files/parents,files/quotaBytesUsed,files/trashed"})))

(defn- get-gdrive-takeout-files

  ([ gdrive-auth ]
   (when-let [takeout-folder (first (query-gdrive-files gdrive-auth "name='Takeout' and mimeType='application/vnd.google-apps.folder'"))]
     (query-gdrive-files gdrive-auth (str "'" (:id takeout-folder) "' in parents"))))

  ([ gdrive-auth takeout-name ]

   (filter #(.startsWith (:name %) (str "takeout-" takeout-name))
           (get-gdrive-takeout-files gdrive-auth))))

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

(defn- get-gdrive-file-by-id [gdrive-auth file-id output-file]
  (log/info "Downloading:" file-id "to" (str output-file))
  (let [buf (byte-array (config/cval :transfer-buffer-size))]
    (with-open [gdrive-stream (get-gdrive-stream gdrive-auth (str "https://www.googleapis.com/drive/v3/files/" file-id)
                                                 {:alt "media"})]
      (with-open [file-stream (clojure.java.io/output-stream output-file)]
        (loop [tot-bytes 0]
          (let [bytes-read (.read gdrive-stream buf)]
            (when (pos? bytes-read)
              (.write file-stream buf 0 bytes-read)
              (recur (+ tot-bytes bytes-read)))))))))


(defn- trash-gdrive-file-by-id [gdrive-auth file-id]
  (log/info "Moving:" file-id "to trash")
  (with-retries
    (http/post-json (str "https://www.googleapis.com/drive/v2/files/" file-id "/trash")
                    :auth gdrive-auth)))

(defn cmd-gdrive-get-file
  "Get a Google Drive file by ID"

  [file-id output-filename]
  (let [gdrive-auth (google-oauth/google-auth-provider)]
    (with-retries
      (get-gdrive-file-by-id gdrive-auth file-id (java.io.File. output-filename)))))

(defn cmd-gdrive-trash-file
  "Trash a Google Drive file by ID"

  [file-id]
  (let [gdrive-auth (google-oauth/google-auth-provider)]
    (trash-gdrive-file-by-id gdrive-auth file-id)))

;;; Google Takeout Support

(defn cmd-gdrive-takeout-list-downloads
  "List available Google Drive downloads produced by Google Takeout."

  []
  (let [gdrive-auth (google-oauth/google-auth-provider)
        downloads (get-gdrive-takeout-downloads gdrive-auth)]
    (doseq [d downloads]
      (println d))))

(defn cmd-gdrive-takeout-list-download-files
  "List available files within a specific named Google Takeout Download."

  [download-name]
  (let [gdrive-auth (google-oauth/google-auth-provider)
        files (get-gdrive-takeout-files gdrive-auth download-name)]

    (file-table (sort-by :name files))))

(defn cmd-gdrive-takeout-sync-download
  "Sync a Google Takeout download to the target  directory."

  [download-name target-path]
  (let [gdrive-auth (google-oauth/google-auth-provider)
        files (get-gdrive-takeout-files gdrive-auth download-name)]

    (doseq [f (sort-by :name files)]
      (if (:trashed f)
        (log/info "Skipping trashed file:" (:id f))
        (with-retries
          (let [file (java.io.File. (str target-path "/" (:name f)))]
            (if (and (.exists file)
                     (= (.length file)
                        (long (bigdec (:quotaBytesUsed f)))))
              (log/info "Skipping file already present locally:" (:id f))
              (get-gdrive-file-by-id gdrive-auth (:id f) file)))
          (trash-gdrive-file-by-id gdrive-auth (:id f)))))))

;;; Subcommand Maps

(def takeout-subcommands
  #^{:doc "Commands for working with Google Takeout photo albums stored in Google Drive."}
  {"ls" #'cmd-gdrive-takeout-list-downloads
   "lsf" #'cmd-gdrive-takeout-list-download-files
   "sync" #'cmd-gdrive-takeout-sync-download})

(def subcommands
  #^{:doc "Commands for interacting with a Google Drive."}
  {"login" #'cmd-gdrive-login
   "logout" #'cmd-gdrive-logout
   "ls" #'cmd-gdrive-list-files
   "get" #'cmd-gdrive-get-file
   "trash" #'cmd-gdrive-trash-file

   "takeout" takeout-subcommands})
