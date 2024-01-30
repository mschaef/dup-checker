(ns dup-checker.fs
  (:use playbook.core
        dup-checker.util)
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [clojure.java.io :as io]
            [dup-checker.catalog :as catalog]
            [dup-checker.store :as store]))

(defn- file-info [ root f ]
  (let [path (.getCanonicalPath f)]
    {:file f
     :full-path path
     :extension (get-file-extension f)
     :last-modified-on (java.util.Date. (.lastModified f))
     :name (.substring path (+ 1 (count (.getCanonicalPath root))))
     :size (.length f)
     :data-stream-fn #(io/input-stream f)}))

(defn- file-path [ & segs ]
  (.normalize
   (.toAbsolutePath (.toPath (apply clojure.java.io/file segs)))))

(defn get-store [ root-path ]
  (reify store/AFileStore
    (get-store-files [ this ]
      (let [ root (clojure.java.io/file root-path)]
        (map #(file-info root %)
             (filter #(.isFile %) (file-seq root)))))

    (get-store-file-path [ this filename ]
      (let [path (file-path root-path filename)]
        (and (.exists (.toFile path))
             path)))

    (link-store-file [ this filename source ]
      (let [target-path (file-path root-path filename)]
        (java.nio.file.Files/createDirectories
         (.getParent target-path)
         (make-array java.nio.file.attribute.FileAttribute 0))

        (java.nio.file.Files/createLink target-path source)))))
