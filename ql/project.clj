(defproject ql "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :profiles {:provided {:dependencies [[org.apache.hadoop/hadoop-common "2.6.3"]
                                       [org.apache.hive/hive-exec "1.2.1"]
                                       [org.apache.hive.hcatalog/hive-hcatalog-core "1.2.1"]]}}
  :aot :all
  :main nil
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.twitter/carbonite "1.4.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.logging "0.3.1"]])

