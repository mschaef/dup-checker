(ns dup-checker.describe
  (:use playbook.core
        sql-file.sql-util
        dup-checker.util)
  (:require [sql-file.middleware :as sfm]
            [taoensso.timbre :as log]))

(defn- all-filenames-by-digest [ ]
  (into {} (map (fn [ value ]
                  [(:md5_digest value) (:name value)])
                (query-all (sfm/db)
                           [(str "SELECT md5_digest, name"
                                 "  FROM file")]))))

(defn- cmd-describe-duplicates
  "List all duplicate files by MD5 digest."
  [ ]

  (let [ md5-to-filename (all-filenames-by-digest)]
    (table
     [:md5_digest :count :name]
     (map #(assoc % :name (md5-to-filename (:md5_digest %)))
          (query-all (sfm/db)
                     [(str "SELECT * FROM ("
                           "   SELECT md5_digest, count(md5_digest) as count"
                           "     FROM file"
                           "    GROUP BY md5_digest)"
                           " WHERE count > 1"
                           " ORDER BY count")])))))

(defn- cmd-describe-digest
  "List every instance of a file with a given MD5 digest."
  [ md5-digest ]
  (table
   [:catalog_name :last_modified_on :size :file_name]
   (query-all (sfm/db)
              [(str "SELECT file.name as file_name, catalog.name as catalog_name, file.size, file.last_modified_on"
                    "  FROM file, catalog"
                    " WHERE md5_digest=?"
                    "  AND file.catalog_id=catalog.catalog_id"
                    " ORDER BY catalog_name")
               md5-digest])))

(defn- cmd-describe-filename
  "List every instance of a file with the given text in its filename."

  [ filename-segment ]
  (table
   [:catalog_name :md5_digest :last_modified_on :size :file_name]
   (query-all (sfm/db)
              [(str "SELECT file.name as file_name, catalog.name as catalog_name, file.size, file.last_modified_on, md5_digest"
                    "  FROM file, catalog"
                    " WHERE INSTR(file.name, ?) > 0"
                    "  AND file.catalog_id=catalog.catalog_id"
                    " ORDER BY catalog_name")
               filename-segment])))

(def subcommands
  {"digest" #'cmd-describe-digest
   "filename" #'cmd-describe-filename
   "duplicates" #'cmd-describe-duplicates})
