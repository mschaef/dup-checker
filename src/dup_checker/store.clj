(ns dup-checker.store)

(defprotocol AFileStore
  "A catalogable store for files."

  (get-store-files [ this ] "Return a sequence of files in the storee."))
