(defproject co.momomo.appsearch "0.2.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :repositories [["jitpack" "https://jitpack.io"]]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :java-source-paths ["src/java"]
  ;:jvm-opts ["-Xmx120G" "-XX:+UseLargePages" "-agentpath:/home/bob/libyjpagent.so"]
  ;:jvm-opts ["-Xmx64G" "-agentpath:/home/bob/libyjpagent.so"]
  :profiles {:dev {:dependencies [[inspector-jay "0.3"]]}
             :apkpure-aws {
                            :main co.momomo.appsearch.scripts.apkpure-aws
                            :aot :all}
             :apkpure-extract {
                                :main co.momomo.appsearch.scripts.apkpure-extract
                                :aot :all}
             :download-apks {:main co.momomo.appsearch.scripts.download-apks}
             :extract-apk-hashes {:main co.momomo.appsearch.scripts.extract-apk-hashes}
             :extract-text {:main co.momomo.appsearch.scripts.extract-text}
             :dedupe-text {:main co.momomo.appsearch.scripts.dedupe-text}
             :pairwise-text-hashes {:main co.momomo.appsearch.scripts.pairwise-text-hashes}
             :compress-apks {:main co.momomo.appsearch.scripts.compress-apks}}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.jsoup/jsoup "1.11.2"]
                 [com.github.yeriomin/play-store-api "0.32"]
                 [clj-http "3.8.0"]
                 [com.google.guava/guava "24.0-jre"]
                 [org.clojure/java.jdbc "0.7.5"]
                 [org.xerial/sqlite-jdbc "3.7.2"]
                 [org.clojure/java.data "0.1.1"]
                 [com.joestelmach/natty "0.11"]
                 [org.apache.commons/commons-compress "1.16.1"]
                 [org.tukaani/xz "1.8"]
                 [org.clojars.kostafey/clucy "0.5.5.0"]
                 [compojure "1.6.0"]
                 [ring/ring-jetty-adapter "1.6.3"]
                 [cheshire "5.8.0"]
                 [net.dongliu/apk-parser "2.5.3"]
                 [org.clojure/data.fressian "0.2.1"]
                 [org.smali/dexlib2 "2.2.3"]
                 [proteus "0.1.6"]
                 [com.amazonaws/aws-java-sdk-s3 "1.11.292"]
                 [org.clojure/data.xml "0.0.8"]
                 [org.asynchttpclient/async-http-client "2.4.4"]
                 [manifold "0.1.6"]
                 [net.sf.trove4j/trove4j "3.0.3"]
                 [xpp3/xpp3 "1.1.4c"]
                 [xmlunit/xmlunit "1.3"]
                 [org.yaml/snakeyaml "1.18"]
                 [org.apache.commons/commons-lang3 "3.1"]])
