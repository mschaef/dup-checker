(ns dup-checker.http
  (:require [taoensso.timbre :as log]
            [clj-http.client :as http]
            [playbook.core :as playbook]))

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

(def get-json (partial http-request-json* http/get))
(def post-json (partial http-request-json* http/post))
