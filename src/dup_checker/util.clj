(ns dup-checker.util
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [again.core :as again]))

(defn fail [ message & args ]
  (let [ full-message (apply str message args)]
    (println (str "Error: " full-message))
    (throw (RuntimeException. full-message))))

(defn table
  ([ rows ]
   (table (keys (first rows)) rows))

  ([ ks rows ]
   (doseq [ p (partition-all 500 rows)]
     (pprint/print-table ks p))
   (println "n=" (count rows))))

(defn get-filename-extension [ name ]
  (let [sep-index (.lastIndexOf name ".")]
    (if (< sep-index 0)
      name
      (.substring name (+ 1 sep-index)))))

(defn get-file-extension [ f ]
  (get-filename-extension (.getName f)))

(defn current-hostname []
  (.getCanonicalHostName (java.net.InetAddress/getLocalHost)))

;; Times in msec
(def retry-policy [ 0 5000 10000 15000 ])

(defmacro with-retries [ & forms ]
  `(again/with-retries retry-policy
     ~@forms))


(defn pretty-spit [filename collection]
  (spit (java.io.File. filename)
        (with-out-str
          (pprint/write collection :dispatch pprint/code-dispatch))))

(defn pretty-slurp [ filename ]
  (clojure.edn/read-string (slurp filename)))

