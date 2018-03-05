(defproject co.momomo.appsearch "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :repositories [["jitpack" "https://jitpack.io"]]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :java-source-paths ["src/java"]
  :profiles {:dev {:dependencies [[inspector-jay "0.3"]]}}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.jsoup/jsoup "1.11.2"]
                 [com.github.yeriomin/play-store-api "0.32"]
                 [clj-http "3.7.0"]
                 [com.google.guava/guava "24.0-jre"]
                 [org.clojure/java.jdbc "0.7.5"]
                 [org.xerial/sqlite-jdbc "3.7.2"]
                 [org.clojure/java.data "0.1.1"]
                 [com.joestelmach/natty "0.11"]
                 [org.tukaani/xz "1.8"]
                 [org.clojars.kostafey/clucy "0.5.5.0"]
                 [compojure "1.6.0"]
                 [ring/ring-jetty-adapter "1.6.3"]
                 [cheshire "5.8.0"]
                 [net.dongliu/apk-parser "2.5.3"]
                 [org.clojure/data.fressian "0.2.1"]])
