(defproject co.momomo.appsearch "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :repositories [["jitpack" "https://jitpack.io"]]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.jsoup/jsoup "1.11.2"]
                 [com.github.yeriomin/play-store-api "0.32"]
                 [org.apache.httpcomponents/httpcore "4.4.9"]
                 [org.apache.httpcomponents/httpclient "4.5.5"]
                 [org.apache.httpcomponents/httpmime "4.5.5"]
                 [com.google.guava/guava "24.0-jre"]
                 [org.clojure/java.jdbc "0.7.5"]
                 [org.xerial/sqlite-jdbc "3.7.2"]
                 [org.clojure/java.data "0.1.1"]
                 [inspector-jay "0.3"]])
