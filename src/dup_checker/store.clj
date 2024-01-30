(ns dup-checker.store
  (:use dup-checker.util
        playbook.config))

(defprotocol AFileStore
  "A catalogable store for files."

  (get-store-files [ this ]
    "Return a sequence of files in the store.")

  (get-store-file-path [ this filename ]
    "Return the path to a store file.")

  (link-store-file [ this filename source ]
    "Create a link from this store to an external path"))

(defn get-store [ uri ]
  (let [scheme (.getScheme uri)
        scheme-specific-part (.getSchemeSpecificPart uri)]
    (if-let [ store (cval :store-scheme scheme)]
      (store scheme-specific-part)
      (fail "Unknown scheme in store URI: " uri))))
