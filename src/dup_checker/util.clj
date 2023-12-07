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

(defn- http-request-json*
  ([ req-fn url ]
   (http-request-json* req-fn url nil))

  ([ req-fn url auth ]
   (let [bearer-token (if (fn? auth) (auth) auth)
         response (req-fn url (if bearer-token
                                {:headers
                                 {:Authorization (str "Bearer " bearer-token)}}
                                {}))]
     (and (= 200 (:status response))
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

