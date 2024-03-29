(ns dup-checker.s3
  (:use playbook.core
        dup-checker.util)
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [clj-http.client :as http]
            [dup-checker.catalog :as catalog]
            [dup-checker.store :as store]))

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

(defn- s3-get-object-attributes [ s3 bucket-name f ]
  (.getObjectAttributes s3 (-> (software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest/builder)
                               (.bucket bucket-name)
                               (.key (.key f))
                               (.build))))

(defn- get-object [ s3 bucket-name f ]
  (.getObject s3 (-> (software.amazon.awssdk.services.s3.model.GetObjectRequest/builder)
                     (.bucket bucket-name)
                     (.key (.key f))
                     (.build))
              (software.amazon.awssdk.core.sync.ResponseTransformer/toInputStream)))

(defn- s3-blob-info [ s3 bucket-name f ]
  {:full-path (.key f)
   :extension (get-filename-extension (.key f))
   :last-modified-on (.lastModified f)
   :name (.key f)
   :size (.size f)
   :data-stream-fn #(get-object s3 bucket-name f)})

(defn get-store [ bucket-name ]
  (let [ s3 (s3-client) ]
    (reify store/AFileStore
      (get-store-files [ this ]
        (map (partial s3-blob-info s3 bucket-name) (s3-list-bucket-paged s3 bucket-name)))

      (get-store-file-path [ this filename ]
        (fail "Cannot get filesystem path to S3 object: " filename))

      (link-store-file [ this filename source ]
        (fail "Cannot create filesystem link in S3:" filename)))))

(defn cmd-s3-list-bucket
  "List the contents of an s3 bucket."
  [ bucket-name ]
  (doseq [ bucket (s3-list-bucket-paged (s3-client) bucket-name)]
    (pprint/pprint bucket)))

(def subcommands
  #^{:doc "AWS S3 subcommands"}
  {"ls" #'cmd-s3-list-bucket})
