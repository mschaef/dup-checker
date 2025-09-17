(ns dup-checker.cli
  (:use playbook.core
        dup-checker.util)
  (:require [playbook.config :as config]
            [taoensso.timbre :as log]))

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

(defn dispatch-subcommand [ cmd-map args ]
  (try
    (if (= (count args) 0)
      (fail "Insufficient arguments, missing subcommand.")
      (let [[ subcommand & args ] args]
        (if-let [ cmd-fn (get (assoc cmd-map "help" #(display-help cmd-map)) subcommand) ]
          (if (map? cmd-fn)
            (dispatch-subcommand cmd-fn args)
            (with-exception-barrier :command-processing
              (apply cmd-fn args)))
          (fail "Unknown subcommand: " subcommand))))
    (catch Exception e
      (display-help cmd-map))))
