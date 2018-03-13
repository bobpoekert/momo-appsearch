(ns co.momomo.appsearch.scripts.apkpure-aws
  (:gen-class)
  (require [co.momomo.appsearch.apkpure :as apkp]
           [co.momomo.appsearch.apk4fun :as apk4]
           [co.momomo.cereal :as cereal]
           [co.momomo.s3 :as s3]
           [co.momomo.appsearch.apk :as apk]
           [clojure.data.fressian :as fress])
  (import [java.util.concurrent LinkedBlockingQueue TimeUnit]
          [org.tukaani.xz XZInputStream XZOutputStream LZMA2Options]
          [java.io ByteArrayOutputStream OutputStream]))

(defn get-download-url
  [mv]
  (or
    (apk4/artifact-download-url (:artifact_name mv))
    (str "https://apkpure.com" (:download_url mv))))

(defn download-and-process-apps-s3!
  [inp-bucket inp-key outp-bucket outp-basename]
  (let [map2 (fn [a b] (map b a))
        filter2 (fn [a b] (filter b a))
        outq (LinkedBlockingQueue. 2)
        download-opts {:headers {"User-Agent" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:58.0) Gecko/20100101 Firefox/58.0"}}
        ^java.util.concurrent.CountDownLatch done-latch
          (->
            (s3/input-stream inp-bucket inp-key)
            (XZInputStream.)
            (cereal/data-seq)
            (cereal/parmap {:core-count 50} (map get-download-url))
            (filter2 #(not (nil? (:url %))))
            (cereal/download (merge download-opts {:as :byte-array}) 100)
            (cereal/queue-seq)
            (cereal/parrun
              (fn [core-id apks]
                (loop [ctr 0 outw nil ^ByteArrayOutputStream bao nil ^OutputStream outs nil apks apks]
                  (cond
                    (nil? apks) (do
                                  (.close outs)
                                  (.put outq (.toByteArray bao))
                                  nil)
                    (> ctr 100) (do 
                                  (.close outs)
                                  (.put outq (.toByteArray bao))
                                  (recur 0 nil nil nil apks))
                    (nil? outw) (let [bao (ByteArrayOutputStream.)
                                      outs (-> bao (XZOutputStream. (LZMA2Options.)))]
                                  (recur 0
                                    (fress/create-writer outs)
                                    bao outs apks))
                    :else (let [[row & rst] apks]
                            (prn (:title (:meta row)))
                            (if (:error row)
                              (prn (:error row))
                              (try
                                (let [apk (apk/load-apk (:body (:result row)))]
                                  (fress/write-object outw
                                    {:meta (:meta row) :apk apk}))
                                (catch Throwable e (prn e))))
                            (recur (inc ctr) outw bao outs rst)))))))]
    (loop [ctr 0]
      (let [v (.poll outq 100 TimeUnit/MILLISECONDS)]
        (if (nil? v)
          (if (.await done-latch 100 TimeUnit/MILLISECONDS)
            nil
            (recur ctr))
          (do
            (s3/upload! outp-bucket
              (str outp-basename "-" ctr)
              v)
            (recur (inc ctr))))))))

(defn -main
  [& args]
  (apply download-and-process-apps-s3! args))
