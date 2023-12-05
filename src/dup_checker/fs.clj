(ns dup-checker.fs
  (:use playbook.core
        dup-checker.util)
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [again.core :as again]
            [clj-commons.digest :as digest]
            [dup-checker.catalog :as catalog]))

;; Times in msec
(def retry-policy [ 0 5000 10000 15000 ])

(defn- file-info [ root f ]
  (let [path (.getCanonicalPath f)]
    {:file f
     :full-path path
     :extension (get-file-extension f)
     :last-modified-on (java.util.Date. (.lastModified f))
     :name (.substring path (+ 1 (count (.getCanonicalPath root))))
     :size (.length f)
     :md5-digest (delay
                   (again/with-retries retry-policy
                     ;; Retry to accomodate potential I/O errors.
                     (digest/md5 f)))}))

(defn- cmd-catalog-fs-files
  "Catalog the contents of an filesystem path."
  [ root-path catalog-name ]

  (let [root (clojure.java.io/file root-path)]
    (catalog/catalog-files (catalog/ensure-catalog catalog-name root-path "fs")
                           (map #(file-info root %) (filter #(.isFile %) (file-seq root))))))

(def subcommands
  #^{:doc "Filesystem subcommands"}
  {"catalog" #'cmd-catalog-fs-files})
