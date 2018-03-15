(ns co.momomo.appsearch.scripts.download-apks
  (require [co.momomo.crawler :as cr]
           [co.momomo.s3 :as s3]
           [co.momomo.cereal :as cereal]
           [co.momomo.appsearch.apkpure :as apkp]
           [co.momomo.appsearch.lieng :as lieng]
           [co.momomo.appsearch.androidappsapk :as aapk]
           [co.momomo.appsearch.apk4fun :as apk4]
           [clojure.string :as ss])
  (import [org.tukaani.xz XZInputStream]))

(defn throw-if-nil
  [v]
  (if (nil? v)
    (throw (RuntimeException.))
    v))

(defn downloader
  [bucket f]
  (fn [info]
    (let [url (f info)]
      (prn url)
      (if (nil? url)
        (throw (RuntimeException.))
        (let [res (cr/req url :get {:as :stream})]
          (when (ss/includes? (get (:headers res) "content-type") "text/html")
            (throw (RuntimeException.)))
            (try
              (s3/stream-http-response! bucket (:artifact_name info) res)
              (catch Exception e (prn e))))))))

(defn requester-fns
  [bucket]
  (->>
    [(comp lieng/download-url :artifact_name)
     (comp aapk/download-url :artifact_name)
     apkp/get-download-url]
    (map (partial downloader bucket))
    (vec)))

(defn download-apps!
  [inp-bucket inp-key outp-bucket]
  (->
    (s3/input-stream inp-bucket inp-key)
    (XZInputStream.)
    (cereal/data-seq)
    (cr/crawl {:core-cnt 100} (requester-fns outp-bucket))))
