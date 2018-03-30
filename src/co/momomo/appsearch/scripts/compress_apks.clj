(ns co.momomo.appsearch.scripts.compress-apks
  (:gen-class)
  (require [co.momomo.s3 :as s3]
           [clj-http.client :as http]
           [clojure.string :as ss]
           [clojure.java.io :as io]
           [clojure.java.shell :refer [sh]])
  (import [java.nio.file Files]
          [java.util.concurrent Executors CountDownLatch ExecutorService]
          [com.amazonaws.services.s3.transfer TransferManagerBuilder TransferManager Download]
          [com.google.common.hash Hashing Hasher]))

(set! *warn-on-reflection* true)

(def manager
  (delay
    (->
      (TransferManagerBuilder/standard)
      (.withS3Client @s3/s3-client)
      (.build))))

(defn compress-dir!
  [dirname outfname]
  (sh "tar" "-Ipixz" "-cf" outfname dirname))

(defn ins-to-file!
  [^java.io.InputStream ins ^java.io.File fd]
  (with-open [w (io/writer fd)]
    (io/copy ins w)))

(defn download-files!
  [bucket fnames outpdir]
  (let [downloads
          (for [fname fnames]
            (.download ^TransferManager @manager
              ^String bucket ^String fname
              (io/file outpdir fname)))]
      (doall downloads)
      (doseq [^Download dl downloads]
        (.waitForCompletion dl))))

(def ^java.nio.charset.Charset utf8 (java.nio.charset.Charset/forName "UTF-8"))

(defn hash-strings
  [strings]
  (let [^Hasher hasher (.newHasher (Hashing/sha256))]
    (doseq [^String s strings]
      (.putString hasher s utf8)
      (.putString hasher "/" utf8))
    (str (.hash hasher))))

(defn compress-bucket!
  [coordinator bucket scratchdir tardir logfile]
  (with-open [logs (io/writer (io/file logfile) :append true)]
    (loop []
      (let [job (:body (http/get (str "http://" coordinator "/get_job") {:as :json}))]
        (when-not (:done job)
          (let [slice (:artifacts job)
                fname (str (hash-strings slice) ".tpxz")]
            (prn fname)
            (download-files! bucket slice scratchdir)
            (compress-dir! scratchdir (str tardir "/" fname))
            (s3/upload-file! bucket fname (io/file tardir fname))
            (prn fname)
            (.delete (io/file tardir fname))
            (doseq [k slice]
              (.delete (io/file scratchdir k))
              (.write logs (format "%s %s\n" fname k))
              (s3/delete! bucket k))
            (.flush logs)
            (recur)))))))

(defn -main
  [& args]
  (apply compress-bucket! args))
