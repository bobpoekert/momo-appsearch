(ns co.momomo.appsearch.scripts.download-apks
  (:gen-class)
  (require [co.momomo.crawler :as cr]
           [co.momomo.s3 :as s3]
           [co.momomo.cereal :as cereal]
           [co.momomo.appsearch.apkpure :as apkp]
           [co.momomo.appsearch.apkd :as apkd]
           [co.momomo.appsearch.lieng :as lieng]
           [co.momomo.appsearch.androidappsapk :as aapk]
           [co.momomo.appsearch.apk4fun :as apk4]
           [clojure.string :as ss]
           [clojure.core.async :as async]
           [co.momomo.async :refer [gocatch]])
  (import [org.tukaani.xz XZInputStream]))

(def errors (atom (list)))

(defn downloader
  [bucket seen f]
  (fn [requester info]
    (gocatch
      (try
        (if (and false (contains? @seen (:artifact_name info)))
          nil
          (let [url (async/<! (f requester info))]
            (if (not (string? url))
              :error
              (let [res (async/<! (cr/req url requester :get {:as :byte-array :socket-timeout 999999}))]
                  (if (or (not (= (:status res) 200)
                               (ss/includes? (get (:headers res) "content-type") "text/html")))
                    :error
                    (do
                      (->
                        (s3/upload! bucket (:artifact_name info) (:body res))
                        (async/thread)
                        (async/<!))
                      (swap! seen (fn [s] (conj s (:artifact_name info))))
                      :success))))))
          (catch Exception e
            (prn (.getMessage e))
            :error)))))

(defn requester-fns
  [bucket seen]
  (->>
    [lieng/download-url
     aapk/download-url 
     apkd/apkd-download-url 
     apkd/apkname-download-url 
     apkd/apkfollow-download-url 
     apkd/apkbird-download-url 
     apkd/apkdl-download-url 
     apkd/apkdroid-download-url 
     apkp/get-download-url]
    (map (partial downloader bucket seen))
    (vec)))

(defn download-apps!
  [inp-bucket inp-key outp-bucket]
  (let [;seen (atom (into #{} (s3/list-bucket outp-bucket)))
        seen (atom #{})
        timeout (System/getProperty "timeout")
        timeout (if timeout (Integer/parseInt timeout) (* 5 60 1000))]
    (->
      (s3/input-stream inp-bucket inp-key)
      (XZInputStream.)
      (cereal/data-seq)
      (cr/crawl {:timeout timeout} (requester-fns outp-bucket seen)))))

(defn -main
  [& args]
  (apply download-apps! args))
