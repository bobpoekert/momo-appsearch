(ns co.momomo.compress
  (require [clojure.java.io :as io])
  (import [org.apache.commons.compress.compressors.xz XZCompressorInputStream XZCompressorOutputStream]
          [org.apache.commons.compress.archivers.tar TarArchiveInputStream TarArchiveEntry]
          [java.io InputStream OutputStream ByteArrayOutputStream]))

(set! *warn-on-reflection* true)

(defn ^bytes slurp-bytes
  [^InputStream ins]
  (let [bao (ByteArrayOutputStream.)]
    (io/copy ins bao)
    (.toByteArray bao)))

(defn ^InputStream xz-input-stream
  [^InputStream in]
  (XZCompressorInputStream. in))

(defn ^OutputStream xz-output-stream
  [^OutputStream out]
  (XZCompressorOutputStream. out))

(defn- inner-tpxz-files
  [^TarArchiveInputStream ins]
  (loop [^TarArchiveEntry entry (.getNextTarEntry ins)]
    (cond
      (nil? entry)
        (do
          (.close ins)
          nil)
      (not (.isFile entry)) (recur (.getNextEntry ins))
      :else (cons {:name (.getName entry)
                   :data (slurp-bytes ins)}
              (lazy-seq (inner-tpxz-files ins))))))

(defn tpxz-files
  [^InputStream ins]
  (->
    ins
    (xz-input-stream)
    (TarArchiveInputStream.)
    (inner-tpxz-files)))

(defn slurp-bytes
  "Slurp the bytes from a slurpable thing"
  [x]
  (with-open [out (java.io.ByteArrayOutputStream.)]
    (io/copy (io/input-stream x) out)
    (.toByteArray out)))

(defn file-datas
  [^String infname]
  (if-not (.endsWith infname ".tpxz")
    [{:name infname :data (slrup-bytes (io/file infname))}]
    (with-open [ins (io/input-stream (io/file infanme))]
      (tpxz-files infname))))
