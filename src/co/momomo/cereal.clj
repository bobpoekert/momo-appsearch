(ns co.momomo.cereal
  (require [clojure.data.fressian :as fress]
           [clojure.java.io :as io]
           [clj-http.client :as http])
  (import [java.util.concurrent LinkedBlockingQueue BlockingQueue]))

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
  ([inp runner]
    (parrun inp runner (fn [] nil)))
  ([inp runner callback]
    (let [inq (if (instance? BlockingQueue inp)
                inp
                (LinkedBlockingQueue. 10))
          done-latch (promise)
          done-cnt (atom 0)
          core-cnt (dec (.availableProcessors (Runtime/getRuntime)))]
      (when-not (identical? inp inq) 
        (thread "parrun generator"
          (partial into-queue! inq inp)))
      (dotimes [core core-cnt]
        (thread "par-process-into-file! worker"
          (fn []
            (runner core (queue-seq inq))
            (swap! done-cnt inc)
            (when (>= @done-cnt core-cnt)
              (deliver done-latch true)
              (callback)))))
      done-latch)))

(defn parmap
  [inp transducer]
  (let [^LinkedBlockingQueue outq (LinkedBlockingQueue. 10)]
    (parrun inp
      (fn [core-id inp]
        (transduce transducer
          (fn [_ v] (.put outq v)) nil inp))
      (partial close-queue! outq))
    (queue-seq outq)))

(defn par-process-into-file!
  [inp transducer outf]
  (with-open [douts (fress/create-writer (io/output-stream outf))]
    (doseq [row (parmap inp transducer)]
      (fress/write-object douts row))))

(defn- inner-data-seq
  [ins]
  (cons
    (fress/read-object ins)
    (lazy-seq (inner-data-seq ins))))

(defn data-seq
  [fd]
  (inner-data-seq (fress/create-reader (io/input-stream fd))))

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
                      (>= @running-count 100))
                (Thread/sleep 10))
              (swap! running-count inc)
              (http/get (:url url) (assoc http-opts :async? true)
                response! response!)))
          (finally (close-queue! outp)))))
    outp)) 
    
