(ns dup-checker.core
  (:require [clj-commons.digest :as digest]))

(defn- file-info [ f ]
  {:name (.getAbsolutePath f)
   :size (.length f)
   :md5-digest (digest/md5 f)
   :sha256-digest (digest/sha256 f)})

(defn- list-files []
  (let [root (clojure.java.io/file ".")]
    (doseq [f (filter #(.isFile %) (file-seq root))]
      (println (file-info f)))))

(defn -main [& args]
  (list-files)
  (println "end run."))
