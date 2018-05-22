(ns co.momomo.appsearch.scripts.extract-apk-hashes
  (:gen-class)
  (require [co.momomo.appsearch.apk :as apk]
           [co.momomo.cereal :as cereal]
           [co.momomo.compress :refer [file-datas]]
           [proteus :refer [let-mutable]]
           [clojure.java.io :as io])
  (import [com.google.common.io LittleEndianDataOutputStream LittleEndianDataInputStream]
          [gnu.trove.set.hash TIntHashSet]
          [gnu.trove.map.hash TIntIntHashMap]
          [gnu.trove.iterator TIntIntIterator]
          [gnu.trove.list.array TIntArrayList]))

(set! *warn-on-reflection* true)

(defn tries
  [thunk]
  (fn [args]
    (try
      (thunk args)
      (catch Throwable e
        (prn e)
        nil))))

(defmacro dofile
  [& bodies]
  `(try (while true ~@bodies)
    (catch java.io.EOFException e# nil)))

(defn give-sequential-ids!
  ([^LittleEndianDataInputStream ins
   ^LittleEndianDataOutputStream outs]
    (let-mutable [ctr 0]
      (let [^TIntIntHashMap ids (TIntIntHashMap.)]
        (dofile
          (let [a (.readInt ins)
                b (.readInt ins)
                c (.readInt ins)
                id (if (.containsKey ids b)
                    (.get ids b)
                    (let [v ctr]
                      (set! ctr (inc v))
                      (.put ids b v)
                      v))]
            (.writeInt outs a)
            (.writeInt outs id)
            (.writeInt outs c)))))))

(defn process-apk
  [v]
  {:name (:name v)
   :hashes (apk/method-hashes (:data v))})

(defn write-coo-bag-vectors!
  [outfname apk-fnames]
  (let [^TIntHashSet seen-once (TIntHashSet.)
        ^TIntHashSet seen-twice (TIntHashSet.)
        ^TIntIntHashMap hash-ids (TIntIntHashMap.)
        namefile (io/file (str outfname ".names"))]
    (with-open [^LittleEndianDataOutputStream outs
                (LittleEndianDataOutputStream. (io/output-stream (io/file outfname)))
                ^java.io.Writer names-out (io/writer namefile)]
      (let-mutable [hash-ctr 0]
        (loop [idx 0
               hists
                  (cereal/parmap apk-fnames
                    {:core-cnt 25}
                    (comp
                      (mapcat file-datas)
                      (map (tries process-apk))))]
          (when (seq hists)
            (let [h (first hists) t (rest hists)
                  n (:name h) h (:hashes h)]
              (.write names-out ^String n)
              (.write names-out "\n")
              (.flush names-out)
              (doseq [pair h]
                (when-not (nil? pair)
                  (let [k (key pair) v (val pair)]
                    (.writeInt outs idx)
                    (.writeInt outs
                      (if (.containsKey hash-ids k)
                        (.get hash-ids k)
                        (let [id hash-ctr]
                          (set! hash-ctr (inc hash-ctr))
                          (.put hash-ids k id)
                          id)))
                    (.writeInt outs v)
                    (if (.contains seen-once (int k))
                      (.add seen-twice (int k))
                      (.add seen-once (int k))))))
              (recur (inc idx) t))))))
      (with-open [outs (LittleEndianDataOutputStream. (io/output-stream (io/file (str outfname ".hashids"))))]
        (let [^TIntIntIterator it (.iterator hash-ids)]
          (while (.hasNext it)
            (.writeInt outs (.key it))
            (.writeInt outs (.value it))
            (.advance it))))))

(defn directory?
  [^java.io.File f]
  (.isDirectory f))

(defn -main
  [inp-dirname outfname]
  (write-coo-bag-vectors! outfname (remove directory? (file-seq (io/file inp-dirname)))))
