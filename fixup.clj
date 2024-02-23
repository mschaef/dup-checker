
(doseq [ catalog (catalog/all-catalogs)]
  (log/info "Resetting exclusions:" catalog)
  (catalog/cmd-catalog-exclude-reset catalog))

(doseq [ catalog (catalog/all-catalogs)]
  (log/info "Excluding from:" catalog)
  (catalog/cmd-catalog-exclude-extension catalog "ithmb" "pdf" "dvi" "tsp" "xcf" "xbm" "xpm" "moi" "mom")

  ;; Remove an icon library, website statistics graphics, and an oracle documentation set.
  (catalog/cmd-catalog-exclude-pattern catalog "%iconic%" "%oracle%" "%_usage%"))

(doseq [ catalog (remove #(= "pictures" %) (catalog/all-catalogs))]
  (log/info "Excluding files already in Lightroom: " catalog)
  (catalog/cmd-catalog-exclude-catalog catalog "pictures"))

(doseq [ catalog (remove #(= "gphoto-takeout-by-year" %) (catalog/all-catalogs))]
  (log/info "Excluding files already in Google Photos: " catalog)
  (catalog/cmd-catalog-exclude-catalog catalog "gphoto-takeout-by-year"))
