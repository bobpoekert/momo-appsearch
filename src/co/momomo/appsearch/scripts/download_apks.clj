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
           [clojure.string :as ss])
  (import [org.tukaani.xz XZInputStream]))

(defn throw-if-nil
  [v]
  (if (nil? v)
    (throw (RuntimeException.))
    v))

(defn downloader
  [bucket seen f]
  (fn [info]
    (do
      (when-not (contains? @seen (:artifact_name info))
        (let [url (f info)]
          (prn [url (:artifact_name info)])
          (if (nil? url)
            (throw (RuntimeException.))
            (let [res (cr/req url :get {:as :byte-array :socket-timeout 999999})]
                (when (ss/includes? (get (:headers res) "content-type") "text/html")
                  (throw (RuntimeException.)))
                  (try
                    ;;(s3/stream-http-response! bucket (:artifact_name info) res)
                    (s3/upload! bucket (:artifact_name info) (:body res))
                    (catch Exception e
                      (do (prn e) (throw e))))
                  (swap! seen (fn [s] (conj s (:artifact_name info))))))))
        nil)))

(defn requester-fns
  [bucket seen]
  (->>
    [(comp lieng/download-url :artifact_name)
     (comp aapk/download-url :artifact_name)
     (comp apkd/apkd-download-url :artifact_name)
     (comp apkd/apkname-download-url :artifact_name)
     apkp/get-download-url]
    (map (partial downloader bucket seen))
    (vec)))

(defn download-apps!
  [inp-bucket inp-key outp-bucket]
  (let [seen (atom (into #{} (s3/list-bucket outp-bucket)))
        cores (System/getProperty "cores")
        cores (if cores (Integer/parseInt cores) 800)]
    (->
      (s3/input-stream inp-bucket inp-key)
      (XZInputStream.)
      (cereal/data-seq)
      (cr/crawl {:core-cnt cores} (requester-fns outp-bucket seen)))))

(defn -main
  [& args]
  (apply download-apps! args))
