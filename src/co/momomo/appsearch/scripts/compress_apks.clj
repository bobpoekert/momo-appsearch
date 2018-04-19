(ns co.momomo.appsearch.scripts.compress-apks
  (:gen-class)
  (require [co.momomo.s3 :as s3]
           [clj-http.client :as http]
           [clojure.string :as ss]
           [clojure.java.io :as io]
           [clojure.java.shell :refer [sh]])
  (import [java.nio.file Files]
          [java.util.concurrent Executors CountDownLatch ExecutorService]
          [java.util.concurrent.atomic AtomicInteger]
          [com.amazonaws.services.s3.transfer
            TransferManagerBuilder TransferManager Download]
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

(def dir-id (AtomicInteger.))

(defn create-scratch-dir!
  [scratch-root]
  (let [id (.incrementAndGet dir-id)
        ^java.io.File fd (io/file scratch-root (str id))]
    (.mkdir fd)
    fd))

(defn job-seq
  [scratch-root coordinator]
  (cons
    (let [job (->
                (str "http://" coordinator "/get_job")
                (http/get {:as :json :conn-timeout 1000})
                (:body)
                (:artifacts))]
      {:dir (create-scratch-dir! scratch-root)
       :tarname (str (hash-strings job) ".tpxz")
       :job job})
    (lazy-seq (job-seq scratch-root coordinator))))

(defn download-mapper
  [bucket]
  (fn [job]
    (prn (:tarname job))
    (download-files! bucket (:job job) (:dir job))
    job))

(defn delete-input!
  [job]
  (doseq [k (:job job)]
    (io/delete-file (io/file (:dir job) k)))
  (io/delete-file (:dir job)))

(defn compress-mapper
  [tardir]
  (fn [job]
    (compress-dir! (str (:dir job)) (str tardir "/" (:tarname job)))
    (delete-input! job)
    job))

(defn upload-mapper
  [bucket tardir]
  (fn [job]
    (s3/upload-file! bucket (:tarname job) (io/file tardir (:tarname job)))
    job))

(defn delete-mapper
  [bucket tardir logs]
  (fn [job]
    (let [fname (:tarname job)]
      (.delete (io/file tardir fname))
      (doseq [k (:job job)]
        (.write logs (format "%s %s\n" fname k))
        (s3/delete! bucket k))
      (.flush logs))))

    
   
(defn consume
  [s]
  (doseq [_ s] nil))


(defn unchunk [s]
  (when (seq s)
    (lazy-seq
      (cons (first s)
            (unchunk (next s))))))

(defn compress-bucket!
  [coordinator bucket scratchdir tardir logfile]
  (with-open [logs (io/writer (io/file logfile) :append true)]
    (->>
      (job-seq scratchdir coordinator)
      (map (download-mapper bucket))
      (seque 1)
      (map (compress-mapper tardir))
      (seque 1)
      (map (upload-mapper bucket tardir))
      (map (delete-mapper bucket tardir logs))
      (consume))))

(defn -main
  [& args]
  (apply compress-bucket! args))
