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
