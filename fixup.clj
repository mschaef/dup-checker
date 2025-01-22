
(doseq [ catalog (catalog/all-catalogs)]
  (log/info "Resetting exclusions:" catalog)
  (catalog/cmd-catalog-exclude-reset catalog))

(doseq [ catalog (catalog/all-catalogs)]
  (log/info "Excluding from:" catalog)
  (catalog/cmd-catalog-exclude-extension catalog "ithmb" "pdf" "dvi" "tsp" "xcf" "xbm" "xpm" "moi" "mom" "mp3" "gif")

  ;; Remove an icon library, website statistics graphics, and an oracle documentation set.
  (catalog/cmd-catalog-exclude-pattern catalog
                                       "%iconic%" "%Iconic%" "%oracle%" "%_usage%" "%JDK%"
                                       "%links-to-media-pc%" "%ectworks%" "%ksm%"
                                       "%iTunes Media%" "%Music%" "%YMCA%" "%np_gym%"
                                       "%utbs%" "%.thumbnails%"))


(doseq [ catalog (remove #(= "pictures" %) (catalog/all-catalogs))]
  (log/info "Excluding files already in Lightroom:" catalog)
  (catalog/cmd-catalog-exclude-catalog catalog "pictures"))

(doseq [ catalog (remove #(= "gphoto-takeout-by-year" %) (catalog/all-catalogs))]
  (log/info "Excluding files already in Google Photos:" catalog)
  (catalog/cmd-catalog-exclude-catalog catalog "gphoto-takeout-by-year"))

(doseq [ catalog (remove #(= "crib-snapshots" %) (catalog/all-catalogs))]
  (log/info "Excluding files already in crib-snapshots:" catalog)
  (catalog/cmd-catalog-exclude-catalog catalog "crib-snapshots"))

;; Using old-external as a baseline for a complete set of missing files.
(doseq [ catalog (remove #(= "old-external" %) (catalog/all-catalogs))]
  (log/info "Excluding files already in old-external:" catalog)
  (catalog/cmd-catalog-exclude-catalog catalog "old-external"))
