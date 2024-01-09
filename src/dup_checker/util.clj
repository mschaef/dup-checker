(ns dup-checker.util
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [again.core :as again]))

(defn fail [ message & args ]
  (let [ full-message (apply str message args)]
    (println (str "Error: " full-message))
    (throw (RuntimeException. full-message))))

(def default-columns
  {:md5_digest 22
   :count 7
   :catalog_name 24
   :name 40
   :size 11
   :gphoto_id 100
   :creation_time 23
   :updated_on 23
   :last_modified_on 23
   :n 8})

(defn- normalize-colspec [ colspec ]
  (if (vector? colspec)
    colspec
    [colspec
     (get default-columns colspec (count (str colspec)) )]))

(defn spaces [ n ]
  (clojure.string/join (repeat n " ")))

(defn- colstr [ colspec row ]
  (let [[ key width ] colspec
        val (str (get row key ""))
        padreq (- width (count val))]
    (if (<= padreq 0)
      val
      (str val (spaces padreq)))))

(defn- print-row [ ks row ]
  (println (clojure.string/join " " (map #(colstr % row) ks))))

(defn table [ ks rows ]
  (let [ks (map normalize-colspec ks)]
    (print-row ks (into {} (map (fn [ [ key _ ] ]
                                  [ key key ]) ks)))
    (doseq [ row rows ]
      (print-row ks row))
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

