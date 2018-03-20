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
          [java.io File OutputStream]))

(def bare-rr (delay (cr/make-requester nil nil nil nil)))

(def temp-dir "/mnt")

(defn download-to-file!
  [url rr outf]
  (let [^OutputStream outs (io/output-stream outf)
        cb (fn [^bytes b] (.write outs b))]
    (d/finally
      (cr/req url rr :get {:body-callback cb}) 
      #(.close outs))))

(defn downloader
  [bucket seen f]
  (fn [requester info]
    (let [^java.io.File outf (java.io.File. (str "/mnt/" (:artifact_name info)))]
      (d/chain
        (f requester info)
        (fn [url]
          (if (not (string? url))
            :error
            (do
              (prn url)
              (d/catch
                (download-to-file! url @bare-rr outf)
                (download-to-file! url requester outf)))))
        (fn [apk-res]
          (if (or (not (= (:status apk-res) 200))
                  (ss/includes? (get (:headers apk-res) "content-type") "text/html"))
            :error
            (do
              (swap! seen (fn [s] (conj s (:artifact_name info))))
              (d/future 
                (prn [bucket (:artifact_name info)])
                (try
                  (s3/upload-file! bucket (:artifact_name info) outf)
                  (finally
                    (.delete outf)))
                :success))))))))

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
     apkd/apkp-download-url
     apkd/apkbiz-download-url]
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
