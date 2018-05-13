(ns co.momomo.appsearch.scripts.extract-apk-hashes
  (:gen-class)
  (require [co.momomo.appsearch.apk :as apk]
           [co.momomo.cereal :as cereal]
           [co.momomo.compress :refer [file-datas]]
           [clojure.java.io :as io])
  (import [com.google.common.io LittleEndianDataOutputStream LittleEndianDataInputStream]
          [gnu.trove.set.hash TIntHashSet]
          [gnu.trove.list.array TIntArrayList]))

(set! *warn-on-reflection* true)

(defn write-coo-bag-vectors!
  [outfname apk-fnames]
  (let [^TIntHashSet seen-once (TIntHashSet.)
        ^TIntHashSet seen-twice (TIntHashSet.)
        tempfile (io/file (str outfname ".tmp"))]
    (with-open [^LittleEndianDataOutputStream outs
                (LittleEndianDataOutputStream. (io/output-stream tempfile))]
      (loop [idx 0
             hists
                (cereal/parmap apk-fnames
                  (map (comp apk/method-hashes apk/load-apk (map :data) (mapcat file-datas))))]
        (when (seq hists)
          (let [h (first hists) t (rest hists)]
            (doseq [pair h]
              (let [k (key pair) v (val pair)]
                (.writeInt outs idx)
                (.writeInt outs k)
                (.writeInt outs v)
                (if (.contains seen-once (int k))
                  (.add seen-twice (int k))
                  (.add seen-once (int k)))))
            (recur (inc idx) t)))))
    (with-open [^LittleEndianDataInputStream ins
                (LittleEndianDataInputStream. (io/input-stream tempfile))]
      (with-open [^LittleEndianDataOutputStream outs
                  (LittleEndianDataOutputStream. (io/output-stream (io/file outfname)))]
        (let [^TIntArrayList ids (TIntArrayList. seen-twice)]
          (with-open [^LittleEndianDataOutputStream id-outs
                      (LittleEndianDataOutputStream. (io/output-stream (io/file (str outfname ".ids"))))]
            (let [^gnu.trove.iterator.TIntIterator it (.iterator ids)]
              (while (.hasNext it)
                (.writeInt id-outs (.next it)))))
          (try
            (while true 
              (let [a (.readInt ins)
                    b (.readInt ins)
                    c (.readInt ins)]
                (when (>= (.binarySearch ids b) 0) 
                  (.writeInt outs a)
                  (.writeInt outs (.binarySearch ids b))
                  (.writeInt outs c))))
            (catch java.io.EOFException e nil)))))
    (.delete tempfile)))

(defn directory?
  [^java.io.File f]
  (.isDirectory f))

(defn -main
  [inp-dirname outfname]
  (write-coo-bag-vectors! outfname (remove directory? (file-seq (io/file inp-dirname)))))
