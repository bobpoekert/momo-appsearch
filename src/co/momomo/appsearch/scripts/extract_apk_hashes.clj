(ns co.momomo.appsearch.scripts.extract-apk-hashes
  (:gen-class)
  (require [co.momomo.appsearch.apk :as apk]
           [co.momomo.cereal :as cereal]
           [clojure.java.io :as io])
  (import [com.google.common.io LittleEndianDataOutputStream]))

(defn slurp-bytes
  "Slurp the bytes from a slurpable thing"
  [x]
  (with-open [out (java.io.ByteArrayOutputStream.)]
    (io/copy (io/input-stream x) out)
    (.toByteArray out)))

(defn write-coo-bag-vectors!
  [outfname apk-fnames]
  (with-open [^LittleEndianDataOutputStream outs (LittleEndianDataOutputStream. (io/output-stream (io/file outfname)))]
    (loop [idx 0
           hists
              (cereal/parmap apk-fnames
                (map (comp apk/method-hashes apk/load-apk slurp-bytes)))]
      (when (seq hists)
        (let [h (first hists) t (rest hists)]
          (doseq [pair h]
            (.writeInt outs idx)
            (.writeInt outs (key pair))
            (.writeInt outs (val pair)))
          (recur (inc idx) t))))))

(defn -main
  [inp-dirname outfname]
  (write-coo-bag-vectors! outfname (file-seq inp-dirname)))
