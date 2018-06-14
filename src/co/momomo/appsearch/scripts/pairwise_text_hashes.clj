(ns co.momomo.appsearch.scripts.pairwise-text-hashes
  (require [clojure.java.io :as io]
           [clojure.string :as ss])
  (import [gnu.trove.map.hash TLongLongHashMap]
          [gnu.trove.iterator TLongLongIterator TIntIterator]
          [gnu.trove.list.array TIntArrayList]
          [gnu.trove.set.hash TIntHashSet]
          [java.util.concurrent CountDownLatch LinkedBlockingQueue]
          [com.google.common.io LittleEndianDataOutputStream]))

(set! *warn-on-reflection* true)

(defn pack-ints
  [a b]
  (let [a (if (> a b) a b)
        b (if (> a b) b a)]
    (bit-or
      (bit-shift-left a 32)
      b)))

(defn add-hashes!
  [^TLongLongHashMap m ^ints hashes]
  (let [stride (unchecked-divide-int (alength hashes) 2)]
    (dotimes [l stride]
      (dotimes [r stride]
        (when-not (= l r)
          (let [k (pack-ints (aget hashes l) (aget hashes (+ r r)))]
            (if (.contains m k)
              (.put m k (inc (.get m k)))
              (.put m k 1))))))))

(defn worker
  [^TLongLongHashMap res ^CountDownLatch latch ^LinkedBlockingQueue inp]
  (try
    (loop [v (.take inp)]
      (when-not (= v :done)
        (add-hashes! res v)
        (recur (.take inp))))
    (finally
      (do
        (prn "done")
        (.countDown latch)
        (.put inp :done)))))


(defmacro dotier
  [[k it] & bodies]
  `(let [it# (if (instance? java.util.Iterator ~it) ~it (.iterator ^Iterable ~it))]
    (while (.hasNext it#)
      (let [~k (.next it#)]
        ~@bodies))))

(defn ^TIntArrayList values-with-key
  [k ^TIntArrayList ks ^TIntArrayList vs]
  (let [^TIntArrayList res (TIntArrayList.)]
    (dotimes [i (.size ks)]
      (when (= (.get ks i) k)
        (.add res ^int (.get vs i))))
    res))

(defn open-file
  [^String infname]
  (if (.endsWith infname ".gz")
    (->
      (io/file infname)
      (io/input-stream)
      (java.util.zip.GZIPInputStream.)
      (java.io.InputStreamReader.)
      (java.io.BufferedReader.))
    (->
      (io/file infname)
      (io/reader)
      (java.io.BufferedReader.))))

(defn -main
  [infname outfname]
  (with-open [^java.io.BufferedReader inf (open-file infname)]
    (let [core-count 18
          ^CountDownLatch latch (CountDownLatch. core-count)
          maps (for [i (range core-count)] (TLongLongHashMap.))
          ^TIntArrayList current-hashes (TIntArrayList.)
          ^TIntArrayList current-keys (TIntArrayList.)
          ^TIntHashSet distinct-keys (TIntHashSet.)
          ^LinkedBlockingQueue job-queue (LinkedBlockingQueue. 10)]
      (doseq [m maps]
        (.start (Thread. ^clojure.lang.IFn (partial worker m latch job-queue))))
      (loop [^String line (.readLine inf)
             ^String prev-appname nil
             cnt 0 chunk-size 0]
        (when-not (nil? line) 
          (let [^objects cols (.split line "\t")
                ^String appname (aget cols 0)
                ^String v (try (str (aget cols 2) "\t" (aget cols 3))
                            (catch ArrayIndexOutOfBoundsException e nil))
                ^int kh (hash (aget cols 1))
                ^int vh (hash v)]
            (when (zero? (mod cnt 1000))
              (prn cnt))
            (cond
              (nil? v) (recur (.readLine inf) prev-appname (inc cnt) chunk-size)
              (= appname prev-appname)
                (do
                  (.add current-hashes vh)
                  (.add current-keys kh)
                  (.add distinct-keys kh)
                  (recur (.readLine inf) appname (inc cnt) (inc chunk-size)))
              :else (do
                      (let [^TIntIterator ks (.iterator distinct-keys)]
                        (while (.hasNext ks)
                          (.put job-queue (.toArray (values-with-key (.next ks) current-keys current-hashes)))))
                      (.resetQuick current-hashes)
                      (.resetQuick current-keys)
                      (.clear distinct-keys)
                      (recur
                        (.readLine inf)
                        appname
                        (inc cnt) 0))))))
      (.put job-queue :done)
      (.await latch)
      (let [^TLongLongHashMap merged (first maps)]
        (doseq [^TLongLongHashMap v (rest maps)]
          (let [^TLongLongIterator it (.iterator v)]
            (while (.hasNext it)
              (try
                (let [k (.key it)]
                  (if (.containsKey merged k)
                    (.put merged k (+ (.get merged k) (.value it)))
                    (.put merged k (.value it))))
                (catch ArrayIndexOutOfBoundsException e nil))
              (.advance it))))
          (let [^TLongLongIterator it (.iterator merged)]
            (with-open [^LittleEndianDataOutputStream outs (LittleEndianDataOutputStream. (io/output-stream (io/file outfname)))]
              (while (.hasNext it)
                (try
                  (do
                    (.writeLong outs (.key it))
                    (.writeLong outs (.value it)))
                  (catch Exception e (prn e)))
                  (.advance it))))))))
        
