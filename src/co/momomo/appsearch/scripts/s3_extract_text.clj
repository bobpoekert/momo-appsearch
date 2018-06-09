(ns co.momomo.appsearch.scripts.s3-extract-text
  (:gen-class)
  (require [co.momomo.appsearch.apk :as apk]
           [co.momomo.cereal :as cereal]
           [co.momomo.s3 :as s3]
           [co.momomo.appsearch.scripts.extract-text :as xt]
           [clojure.java.io :as io]
           [clojure.string :as ss])
  (import [java.io ByteArrayInputStream ByteArrayOutputStream]))
        
(defn load-ins
  [ins]
  (let [bao (ByteArrayOutputStream.)]
    (io/copy ins bao)
    (ByteArrayInputStream. (.toByteArray bao))))

(defn extract-stream-text
  [seen bucket keyname]
  (if (contains? @seen keyname)
    (do
      (prn "skip")
      [nil []])
    (with-open [ins (s3/input-stream bucket keyname)]
      (prn keyname)
      (try
        (let [res (apk/get-strings (load-ins ins))]
          (swap! seen #(conj % keyname))
          [keyname res])
        (catch Exception e
          [nil []])))))

(defn extract-text!
  [outfname bucket-name]
  (with-open [^java.io.Writer outf (java.io.OutputStreamWriter. (java.util.zip.GZIPOutputStream. (io/output-stream (io/file outfname))))]
    (let [bucket-keys (s3/list-bucket bucket-name)
          seen (atom #{})]
      (doseq [[n part] (cereal/parmap bucket-keys
                        {:core-cnt 10}
                        (map (partial extract-stream-text seen bucket-name)))]
        (prn n)
        (doseq [e part]
          (xt/write-part! outf n e))))))

(defn -main
  [bucket outfname]
  (extract-text! outfname bucket))
