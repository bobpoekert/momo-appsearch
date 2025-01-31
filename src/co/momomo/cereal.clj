(ns co.momomo.cereal
  (require [clojure.data.fressian :as fress]
           [clojure.java.io :as io]
           [clj-http.client :as http]
           [co.momomo.compress :refer [xz-input-stream xz-output-stream]])
  (import [java.util.concurrent
            LinkedBlockingQueue BlockingQueue CountDownLatch TimeUnit]
          [java.util List]))

(defn queue-seq
  [^LinkedBlockingQueue inp]
  (loop [v (.take inp)]
    (cond 
      (instance? Throwable v)
        (do
          (prn v)
          (recur (.take inp)))
      (= v ::closed)
        (do
          (.put inp ::closed)
          nil)
      :else (cons v (lazy-seq (queue-seq inp))))))

(defn close-queue!
  [^LinkedBlockingQueue q]
  (.put q ::closed))

(defn into-queue!
  [^LinkedBlockingQueue q ins]
  (do
    (doseq [v ins]
      (when-not (nil? v)
        (.put q v)))
    (close-queue! q)))

(defn thread
  [n f]
  (doto
    (Thread. f n)
    (.start)))

(def ^:dynamic *core-id* nil)

(defn swallow-exc
  [x tt]
  (fn
    ([acc v]
      (try
        (tt acc v)
        (catch Throwable e
          (if (instance? x e)
            acc
            (throw e)))))
    ([v] (tt v))))

(defn get-core-cnt
  [opts]
  (if (:core-cnt opts)
    (:core-cnt opts)
    (dec (.availableProcessors (Runtime/getRuntime)))))

(defn parrun
  ([inp opts runner]
    (let [inq (if (instance? BlockingQueue inp)
                inp
                (LinkedBlockingQueue. 10))
          core-cnt (get-core-cnt opts)
          done-latch (CountDownLatch. core-cnt)
          threads (java.util.ArrayList.)]
      (when-not (identical? inp inq) 
        (thread "parrun generator"
          (partial into-queue! inq inp)))
      (dotimes [core core-cnt]
        (.add threads
          (thread "parrun worker"
            (fn []
              (try
                (binding [*core-id* core]
                  (runner core (queue-seq inq)))
                (finally
                  (do
                    (.countDown done-latch)
                    (when (and
                            (:callback opts)
                            (.await done-latch 1 TimeUnit/MILLISECONDS))
                      ((:callback opts))))))))))
      {:latch done-latch
       :threads threads}))
  ([inp runner]
    (parrun inp {} runner)))

(defn parmap
  ([inp opts transducer]
    (let [^LinkedBlockingQueue outq (LinkedBlockingQueue. 10)
          timeout (get opts :timeout 120000)
          heartbeats (atom {})
          start-latch (CountDownLatch. (get-core-cnt opts))
          threads (parrun inp
                    (assoc opts :callback (partial close-queue! outq))
                    (fn runner [core-id inp]
                      (.countDown start-latch)
                      (swap! heartbeats #(assoc % core-id (System/currentTimeMillis)))
                      (try
                        (transduce (swallow-exc InterruptedException transducer)
                          (fn transducer-cb
                            ([v] 
                              (when-not (nil? v)
                                (.put outq v))
                              (swap! heartbeats #(assoc % core-id (System/currentTimeMillis))))
                            ([_ v] (transducer-cb v))) nil inp)
                        (catch Throwable e
                          (do
                            (.put outq e)
                            (runner core-id inp))))))]
        (thread "heartbeat watchdog"
          (fn []
            (Thread/sleep 1000)
            (.await start-latch)
            (while (not (.await ^CountDownLatch (:latch threads) 100 TimeUnit/MILLISECONDS))
              (dotimes [core-id (.size ^List (:threads threads))]
                (let [last-hb (get @heartbeats core-id)]
                  (when (and (not (nil? last-hb)) (> (- (System/currentTimeMillis) last-hb) timeout))
                    (.interrupt (.get ^List (:threads threads) core-id))))))))
        (queue-seq outq)))
  ([inp transducer]
    (parmap inp {} transducer)))

(defn data-into-file!
  [inp outf]
  (with-open [outs (xz-output-stream (io/output-stream outf))]
    (let [douts (fress/create-writer outs)]
      (doseq [row inp]
        (fress/write-object douts row)))))

(defn par-process-into-file!
  ([inp opts transducer outs]
    (let [douts (fress/create-writer outs)]
      (doseq [row (parmap inp opts transducer)]
        (fress/write-object douts row))))
  ([inp transducer outf]
    (par-process-into-file! inp {} transducer outf)))

(defn- inner-data-seq
  [ins]
  (try
    (cons
      (fress/read-object ins)
      (lazy-seq (inner-data-seq ins)))
    (catch java.io.EOFException e nil)))

(defn data-seq
  [fd]
  (inner-data-seq (fress/create-reader (io/input-stream fd))))

(defn xz-data-seq
  [fd]
  (->
    (io/input-stream fd)
    (xz-input-stream)
    (fress/create-reader)
    (inner-data-seq)))

(defn download
  [urls http-opts queue-size]
  (let [outp (LinkedBlockingQueue. queue-size)
        running-count (atom 0)
        response! (fn [m v]
                    (swap! running-count dec)
                    (.put outp {:meta m :result v}))]
    (thread "download adder"
      (fn []
        (try
          (doseq [url urls]
            (let [response! (partial response! (:meta url))]
              (while (or
                      (not (nil? (.peek outp)))
                      (>= @running-count 10))
                (Thread/sleep 10))
              (swap! running-count inc)
              (try
                (http/get (:url url) (assoc http-opts :async? true)
                  response! response!)
                (catch Throwable e
                  (.put outp {:meta (:meta url) :error e})))))
          (catch Exception e (prn e))
          (finally (close-queue! outp)))))
    outp)) 
    
