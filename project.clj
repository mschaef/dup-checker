(defproject dup-checker "0.1.0-SNAPSHOT"
  :description "Duplicate file checking tool"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [listora/again "1.0.0"]
                 [org.clj-commons/digest "1.4.100"]
                 [com.mschaef/sql-file "0.4.8"]
                 [clj-http "3.12.3"
                  :exclusions [com.fasterxml.jackson.core/jackson-core]]
                 ;; TODO: This brings in an awful lot for a CLI app.
                 [com.mschaef/playbook "0.0.10"]]

  :repl-options {:init-ns dup-checker.core}

  :main dup-checker.core

  :jvm-opts ["-Dconf=local-config.edn"]

  :jar-name "dup-checker.jar"
  :uberjar-name "dup-checker-standalone.jar"

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "--no-sign"]
                  ["uberjar"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]])
