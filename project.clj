(defproject active-escsv "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/core.async "0.4.474"]
                 [org.clojure/test.check "0.9.0"]

                 [org.clojure/tools.cli "0.4.1"]
                 [com.taoensso/timbre "4.10.0"]
                 [org.slf4j/slf4j-log4j12 "1.7.9"]

                 [cc.qbits/spandex "0.7.1"]
                 [clj-time "0.15.0"]

                 [clojure-csv/clojure-csv "2.0.1"]]
  :main ^:skip-aot active-escsv.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
