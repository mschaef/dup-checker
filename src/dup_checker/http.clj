(ns dup-checker.http
  (:require [taoensso.timbre :as log]
            [clj-http.client :as http]
            [playbook.core :as playbook]))

(defn- http-request* [ req-fn return-as url & {:keys [ auth ]} ]
  (let [bearer-token (if (fn? auth) (auth) auth)]
    (let [response (req-fn url
                           (cond-> {:as return-as
                                    :socket-timeout 15000
                                    :connection-timeout 15000}
                             bearer-token (assoc :oauth-token bearer-token)))]
      (and (= 200 (:status response))
           (:body response)))))

(def get-json (partial http-request* http/get :json))
(def post-json (partial http-request* http/post :json))

(def get-binary-stream (partial http-request* http/get :stream))
