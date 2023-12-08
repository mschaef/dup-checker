(ns dup-checker.fs
  (:use playbook.core
        dup-checker.util)
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [clojure.java.io :as io]
            [dup-checker.catalog :as catalog]))

(defn- file-info [ root f ]
  (let [path (.getCanonicalPath f)]
    {:file f
     :full-path path
     :extension (get-file-extension f)
     :last-modified-on (java.util.Date. (.lastModified f))
     :name (.substring path (+ 1 (count (.getCanonicalPath root))))
     :size (.length f)
     :data-stream-fn #(io/input-stream f)}))

(defn- cmd-catalog-fs-files
  "Catalog the contents of an filesystem path."
  [ root-path catalog-name ]

  (let [root (clojure.java.io/file root-path)]
    (catalog/catalog-files (catalog/ensure-catalog catalog-name root-path "fs")
                           (map #(file-info root %) (filter #(.isFile %) (file-seq root))))))

(def subcommands
  #^{:doc "Filesystem subcommands"}
  {"catalog" #'cmd-catalog-fs-files})
