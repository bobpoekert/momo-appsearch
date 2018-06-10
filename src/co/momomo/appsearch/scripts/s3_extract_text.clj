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
  [seen-name outfname bucket-name]
  (with-open [^java.io.Writer outf (->
                                    (io/file outfname)
                                    (io/output-stream)
                                    (java.util.zip.GZIPOutputStream.)
                                    (java.io.OutputStreamWriter.))]
    (let [bucket-keys (drop-while #(not (= % "com.tiens") )(s3/list-bucket bucket-name))
          seen (with-open [ss (io/reader (io/file seen-name))]
                (atom (into #{} (map (fn [^String v] (.trim v)) (line-seq ss)))))]
      (doseq [[n part] (cereal/parmap bucket-keys
                        {:core-cnt 10}
                        (map (partial extract-stream-text seen bucket-name)))]
        (when-not (nil? n)
          (prn n))
        (doseq [e part]
          (xt/write-part! outf n e))))))

(defn -main
  [& args]
  (apply extract-text! args))
