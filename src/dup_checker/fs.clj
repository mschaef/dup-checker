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


(defn get-store [ root-path ]
  (reify catalog/AFileStore
    (get-store-files [ this ]
      (let [ root (clojure.java.io/file root-path)]
        (map #(file-info root %)
             (filter #(.isFile %) (file-seq root)))))))
