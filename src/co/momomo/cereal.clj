(ns co.momomo.cereal
  (require [clojure.data.fressian :as fress]
           [clojure.java.io :as io]
           [clj-http.client :as http])
  (import [java.util.concurrent
            LinkedBlockingQueue BlockingQueue CountDownLatch TimeUnit]
          [org.tukaani.xz XZInputStream]))

(defn queue-seq
  [^LinkedBlockingQueue inp]
  (let [v (.take inp)]
    (if (= v ::closed)
      (do
        (.put inp ::closed)
        nil)
      (cons v (lazy-seq (queue-seq inp))))))

(defn close-queue!
  [^LinkedBlockingQueue q]
  (.put q ::closed))

(defn into-queue!
  [^LinkedBlockingQueue q ins]
  (do
    (doseq [v ins]
      (.put q v))
    (close-queue! q)))

(defn thread
  [n f]
  (doto
    (Thread. f n)
    (.start)))

(defn parrun
  ([inp opts runner]
    (let [inq (if (instance? BlockingQueue inp)
                inp
                (LinkedBlockingQueue. 10))
          core-cnt (:core-cnt opts)
          core-cnt (if (nil? core-cnt)
                    (dec (.availableProcessors (Runtime/getRuntime)))
                    core-cnt)
          done-latch (CountDownLatch. core-cnt)]
      (when-not (identical? inp inq) 
        (thread "parrun generator"
          (partial into-queue! inq inp)))
      (dotimes [core core-cnt]
        (thread "parrun worker"
          (fn []
            (try
              (runner core (queue-seq inq))
              (finally
                (do
                  (.countDown done-latch)
                  (when (and
                          (:callback opts)
                          (.await done-latch 1 TimeUnit/MILLISECONDS))
                    ((:callback opts)))))))))
      done-latch))
  ([inp runner]
    (parrun inp {} runner)))

(defn parmap
  ([inp opts transducer]
    (let [^LinkedBlockingQueue outq (LinkedBlockingQueue. 10)
          ^CountDownLatch latch
            (parrun inp
              (assoc opts :callback (partial close-queue! outq))
              (fn [core-id inp]
                (transduce transducer
                  (fn [_ v] (.put outq v)) nil inp)))]
      (queue-seq outq)))
  ([inp transducer]
    (parmap inp {} transducer)))

(defn par-process-into-file!
  ([inp opts transducer outf]
    (with-open [douts (fress/create-writer (io/output-stream outf))]
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
    (XZInputStream.)
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
    
