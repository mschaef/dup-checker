(ns dup-checker.cli
  (:use playbook.core
        dup-checker.util)
  (:require [playbook.config :as config]
            [taoensso.timbre :as log]
            [clojure.tools.cli :refer [parse-opts]]))

(defn- command-info [cmd-map cmd-name]
  (let [cmd-fn (get cmd-map cmd-name)
        { doc :doc arglists :arglists } (meta cmd-fn) ]
    (if (map? cmd-fn)
      {:type :subcategory :description (str cmd-name (if doc (str " - " doc) ""))}
      {:type :command :description (str cmd-name " " (first arglists) (if doc (str " - " doc) ""))})))

(defn- display-subcommand-group [infos label]
  (when (> (count infos) 0)
    (println (str "\n" label ":"))
    (doseq [info infos]
      (println (str "  " (:description info))))))

(defn- display-help [ cmd-map ]
  (let [grouped-commands
        (group-by :type (map (partial command-info cmd-map) (sort (keys cmd-map))))]
    (display-subcommand-group (:command grouped-commands) "Commands")
    (display-subcommand-group (:subcategory grouped-commands) "Subcategories")))

(defn apply-subcommand [ cmd-fn opts args ]
  (with-exception-barrier :command-processing
    (let [{:keys [options arguments errors summary]} (parse-opts args opts)]
      (if (nil? errors)
        (config/with-extended-config options
          (apply cmd-fn arguments))
        (doseq [error errors]
          (println (str "Error: " error)))))))

(defn dispatch-subcommand [cmd-map args]
  (loop [cmd-map cmd-map
         opts (get (meta cmd-map) :opts [])
         args args]
    (letfn [(error [msg]
              (println (str "Error: " msg))
              (display-help cmd-map))]
      (if (= (count args) 0)
        (error "Insufficient arguments, missing subcommand.")
        (let [[ subcommand & args ] args]
          (if-let [ cmd-fn (get (assoc cmd-map "help" #(display-help cmd-map)) subcommand) ]
            (let [opts (concat opts (get (meta cmd-fn) :opts []))]
              (if (map? cmd-fn)
                (recur cmd-fn opts args)
                (apply-subcommand cmd-fn opts args)))
            (error (str "Unknown subcommand: " subcommand))))))))
