(ns dup-checker.s3
  (:use playbook.core
        dup-checker.util)
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [clj-http.client :as http]
            [dup-checker.catalog :as catalog]))

(defn- s3-client []
  (-> (software.amazon.awssdk.services.s3.S3Client/builder)
      (.region software.amazon.awssdk.regions.Region/US_EAST_1)
      (.build)))

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

  (catalog/catalog-files (catalog/ensure-catalog catalog-name bucket-name "s3")
                         (map s3-blob-info (s3-list-bucket-paged (s3-client) bucket-name))))

(defn- cmd-list-s3-bucket
  "List the contents of an s3 bucket."
  [ bucket-name ]
  (doseq [ bucket (s3-list-bucket-paged (s3-client) bucket-name)]
    (pprint/pprint bucket)))

(def subcommands
  #^{:doc "AWS S3 subcommands"}
  {"ls" #'cmd-list-s3-bucket
   "catalog" #'cmd-catalog-s3-files})
