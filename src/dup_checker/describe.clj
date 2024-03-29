(ns dup-checker.describe
  (:use playbook.core
        sql-file.sql-util
        dup-checker.util)
  (:require [sql-file.middleware :as sfm]
            [taoensso.timbre :as log]
            [dup-checker.catalog :as catalog]))

(defn cmd-describe-duplicates
  "List all duplicate files by MD5 digest."
  [ ]

  (let [ md5-to-filename (catalog/all-filenames-by-digest)]
    (table
     [:md5_digest :count :name]
     (map #(assoc % :name (md5-to-filename (:md5_digest %)))
          (query-all (sfm/db)
                     [(str "SELECT * FROM ("
                           "   SELECT md5_digest, count(md5_digest) as count"
                           "     FROM file"
                           "    WHERE NOT excluded"
                           "    GROUP BY md5_digest)"
                           " WHERE count > 1"
                           " ORDER BY count")])))))

(defn- describe-digest
  "List every instance of a file with a given MD5 digest."
  [ md5-digest ]
  (println "Digest: " md5-digest)
  (table
   [:catalog_name :last_modified_on :size :file_name]
   (query-all (sfm/db)
              [(str "SELECT file.name as file_name, catalog.name as catalog_name,"
                    "       file.size, file.last_modified_on"
                    "  FROM file, catalog"
                    " WHERE md5_digest=?"
                    "  AND file.catalog_id=catalog.catalog_id"
                    "  AND NOT file.excluded"
                    " ORDER BY catalog_name")
               md5-digest]))
  (println))


(defn cmd-describe-digest
  "List every instance of a file with the given MD5 digests."
  [ & md5-digests ]
  (doseq [ md5-digest md5-digests ]
    (describe-digest md5-digest)))

(defn- describe-filename
  "List every instance of a file with the given text in its filename."

  [ filename-segment ]
  (println "Filename Segment: " filename-segment)
  (table
   [:catalog_name :md5_digest :last_modified_on :size :file_name]
   (query-all (sfm/db)
              [(str "SELECT file.name as file_name, catalog.name as catalog_name, file.size, file.last_modified_on, md5_digest"
                    "  FROM file, catalog"
                    " WHERE INSTR(file.name, ?) > 0"
                    "   AND file.catalog_id=catalog.catalog_id"
                    "   AND NOT file.excluded"
                    " ORDER BY catalog_name")
               filename-segment]))
  (println))

(defn cmd-describe-filename
  "List every instance of a file with any of the given text strings in its filename."

  [ & filename-segments ]
  (doseq [ filename-segment filename-segments ]
    (describe-filename filename-segment)))

(def subcommands
  {"digest" #'cmd-describe-digest
   "filename" #'cmd-describe-filename
   "duplicates" #'cmd-describe-duplicates})
