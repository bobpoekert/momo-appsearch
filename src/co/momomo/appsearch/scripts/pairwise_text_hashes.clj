(ns co.momomo.appsearch.scripts.pairwise-text-hashes
  (require [clojure.java.io :as io]
           [clojure.string :as ss])
  (import [gnu.trove.map.hash TLongLongHashMap]
          [gnu.trove.iterator TLongLongIterator]
          [com.google.common.io LittleEndianDataOutputStream]))

(set! *warn-on-reflection* true)

(defn pack-ints
  [a b]
  (bit-and
    (bit-shift-left a 32)
    b))

(defn -main
  [infname outfname]
  (with-open [^java.io.BufferedReader inf (java.io.BufferedReader. (io/reader (io/file infname)))]
    (let [^TLongLongHashMap pair-counts (TLongLongHashMap.)
          ^ints current-hashes (int-array 1000000)]
      (loop [^String line (.readLine inf)
             ^String prev-appname nil
             hashes-idx 0]
        (when-not (nil? line)
          (let [^objects cols (.split line "\t")
                ^String appname (aget cols 0)
                ^String v (try (str (aget cols 2) "\t" (aget cols 3))
                            (catch ArrayIndexOutOfBoundsException e nil))
                ^int vh (hash v)]
            (cond
              (nil? v) (recur (.readLine inf) prev-appname hashes-idx)
              (= appname prev-appname)
                (recur (.readLine inf) appname (inc hashes-idx))
              :else (do
                      (dotimes [l hashes-idx ]
                        (dotimes [r hashes-idx]
                          (let [k (pack-ints (aget current-hashes l) (aget current-hashes r))]
                            (if (.contains pair-counts k)
                              (.put pair-counts k (inc (.get pair-counts k)))
                              (.put pair-counts k 1)))))
                      (aset current-hashes 0 vh)
                      (recur
                        (.readLine inf)
                        appname
                        1))))))
      (let [^TLongLongIterator it (.iterator pair-counts)]
        (with-open [^LittleEndianDataOutputStream outs (LittleEndianDataOutputStream. (io/output-stream (io/file outfname)))]
          (while (.hasNext it)
            (.writeLong outs (.key it))
            (.writeLong outs (.value it))
            (.advance it)))))))
        
