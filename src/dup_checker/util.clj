(ns dup-checker.util
  (:require [clojure.pprint :as pprint]
            [taoensso.timbre :as log]
            [clj-http.client :as http]
            [playbook.core :as playbook]))

(defn fail [ message & args ]
  (let [ full-message (apply str message args)]
    (println (str "Error: " full-message))
    (throw (RuntimeException. full-message))))

(defn table
  ([ rows ]
   (table (keys (first rows)) rows))

  ([ ks rows ]
   (pprint/print-table ks rows)
   (println "n=" (count rows))))

(defn- http-request-json* [ req-fn url & {:keys [ auth as-binary-stream ]} ]
  (let [bearer-token (if (fn? auth) (auth) auth)
        response (req-fn url
                         (cond-> {}
                           bearer-token (assoc :headers
                                               {:Authorization (str "Bearer " bearer-token)})
                           as-binary-stream (assoc :as :stream)))]
    (and (= 200 (:status response))
         (if as-binary-stream
           (:body response)
           (playbook/try-parse-json (:body response))))))

(def http-get-json (partial http-request-json* http/get))
(def http-post-json (partial http-request-json* http/post))

(defn get-file-extension [ f ]
  (let [name (.getName f)
        sep-index (.lastIndexOf name ".")]
    (if (< sep-index 0)
      name
      (.substring name (+ 1 sep-index)))))

(defn current-hostname []
  (.getCanonicalHostName (java.net.InetAddress/getLocalHost)))

