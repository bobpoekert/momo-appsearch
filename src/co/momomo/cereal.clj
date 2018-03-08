(ns co.momomo.cereal
  (require [clojure.data.fressian :as fress]
           [clojure.java.io :as io])
  (import [java.util.concurrent LinkedBlockingQueue]))

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

(defn par-process-into-file!
  [transducer inp outf]
  (with-open [douts (fress/create-writer (io/output-stream outf))]
    (let [inq (LinkedBlockingQueue. 10)
          ^LinkedBlockingQueue outq (LinkedBlockingQueue. 10)]
      (thread "par-process-into-file! generator"
        (partial into-queue! inq inp))
      (dotimes [core (dec (.availableProcessors (Runtime/getRuntime)))]
        (thread "par-process-into-file! worker"
          (fn []
            (do
              (transduce transducer
                (fn [_ v] (.put outq v)) nil
                (queue-seq inq))
              (close-queue! outq)))))
        (doseq [res (queue-seq outq)]
          (fress/write-object douts res)))))

(defn- inner-data-seq
  [ins]
  (cons
    (fress/read-object ins)
    (lazy-seq (inner-data-seq ins))))

(defn data-seq
  [fd]
  (inner-data-seq (fress/create-reader (io/input-stream fd))))
