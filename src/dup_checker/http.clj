(ns dup-checker.http
  (:require [taoensso.timbre :as log]
            [clj-http.client :as http]
            [playbook.core :as playbook]))

(defn- http-request-json* [ req-fn url & {:keys [ auth ]} ]
  (let [bearer-token (if (fn? auth) (auth) auth)]
    (let [response (req-fn url
                           (cond-> {}
                             bearer-token (assoc :headers
                                                 {:Authorization (str "Bearer " bearer-token)})))]
      (and (= 200 (:status response))
           (playbook/try-parse-json (:body response))))))

(def get-json (partial http-request-json* http/get))
(def post-json (partial http-request-json* http/post))

(defn get-binary-stream [ url & {:keys [ auth ]} ]
  (let [bearer-token (if (fn? auth) (auth) auth)]
    (let [response (http/get url
                             (cond-> {:as :stream}
                               bearer-token (assoc :headers
                                                   {:Authorization (str "Bearer " bearer-token)})))]
      (and (= 200 (:status response))
           (:body response)))))

