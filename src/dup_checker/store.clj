(ns dup-checker.store
  (:use dup-checker.util
        playbook.config))

(defprotocol AFileStore
  "A catalogable store for files."

  (get-store-files [ this ] "Return a sequence of files in the store.")

  (link-store-file [ this file ] "Create a link from this store to a file in another."))

(defn get-store [ uri ]
  (let [scheme (.getScheme uri)
        scheme-specific-part (.getSchemeSpecificPart uri)]
    (if-let [ store (cval :store-scheme scheme)]
      (store scheme-specific-part)
      (fail "Unknown scheme in store URI: " uri))))
