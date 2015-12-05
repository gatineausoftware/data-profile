(defproject data-profile "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
 :dependencies [[org.clojure/clojure "1.6.0"]
                [gorillalabs/sparkling "1.2.2-SNAPSHOT"]
                [clojure-csv/clojure-csv "2.0.1"]
                [clj-time "0.8.0"]
                [org.clojure/tools.cli "0.3.3"]
                ]

 :aot [#".*" sparkling.serialization sparkling.destructuring]
 :main data-profile.core
 :profiles {:provided {:dependencies [[org.apache.spark/spark-core_2.10 "1.3.1"]]}
            :dev {:plugins [[lein-dotenv "RELEASE"]
                            [lein-midje "3.1.3"]]
                  :dependencies [[midje "1.5.1"]]}})

