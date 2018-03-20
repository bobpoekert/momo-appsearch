(ns co.momomo.appsearch.scripts.download-apks
  (:gen-class)
  (require [co.momomo.crawler :as cr]
           [co.momomo.s3 :as s3]
           [co.momomo.cereal :as cereal]
           [co.momomo.appsearch.apkd :as apkd]
           [clojure.string :as ss]
           [clojure.java.io :as io]
           [manifold.deferred :as d])
  (import [org.tukaani.xz XZInputStream]
          [java.util.concurrent.atomic AtomicReference]))

(defn downloader
  [bucket seen f]
  (fn [requester info]
    (d/chain
      (f requester info)
      (fn [url]
        (if (not (string? url))
          :error
          (do (prn url)
            (cr/req url requester :get {:as :byte-array}))))
      (fn [apk-res]
        (if (or (not (= (:status apk-res) 200))
                (ss/includes? (get (:headers apk-res) "content-type") "text/html"))
          :error
          (do
            (future 
              (prn [bucket (:artifact_name info)])
              (s3/upload! bucket (:artifact_name info) (:body apk-res)))
            (swap! seen (fn [s] (conj s (:artifact_name info))))
            :success))))))

(defn requester-fns
  [bucket seen]
  (->>
    [;apkd/lieng-download-url
     apkd/aapk-download-url 
     apkd/apkd-download-url 
     ;apkd/apkname-download-url 
     apkd/apkfollow-download-url 
     apkd/apkbird-download-url 
     apkd/apkdl-download-url 
     apkd/apkdroid-download-url 
     apkd/apkp-download-url]
    (map (partial downloader bucket seen))
    (vec)))

(defn inner-download-apps!
  [inp-stream  outp-bucket]
  (let [seen (atom (into #{} (s3/list-bucket outp-bucket)))]
        ;seen (atom #{})
    (->
      (filter #(not (contains? @seen (:artifact_name %)))
        (->
          inp-stream
          (XZInputStream.)
          (cereal/data-seq)))
      (cr/crawl {} (requester-fns outp-bucket seen)))))

(defn download-apps!
  ([inp-bucket inp-key outp-bucket]
    (inner-download-apps! (s3/input-stream inp-bucket inp-key) outp-bucket))
  ([inp-fname outp-bucket]
    (inner-download-apps! (io/input-stream (java.io.File. inp-fname)) outp-bucket)))

(defn -main
  [& args]
  (apply download-apps! args))
