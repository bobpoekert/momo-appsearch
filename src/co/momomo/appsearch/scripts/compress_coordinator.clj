(ns co.momomo.appsearch.scripts.compress-coordinator
  (:gen-class)
  (require [co.momomo.s3 :as s3]
           [cheshire.core :as json]
           [ring.adapter.jetty :refer [run-jetty]]
           [clojure.string :as ss]
           [clojure.java.io :as io]))

(defn inner-key-partitions
  [summaries]
  (when (seq summaries)
    (loop [slice (list)
           size 0
           rst summaries]
      (cond
        (not (seq rst)) slice
        (< (+ size (.getSize (first rst))) (* 4.5 1000 1000 1000))
          (let [h (first rst)]
            (recur (cons h slice) (+ size (.getSize h)) (rest rst)))
        :else (cons slice (lazy-seq (inner-key-partitions rst)))))))

(defn key-partitions
  [bucket seen]
  (let [summaries (s3/list-bucket-summaries bucket)
        summaries (remove #(ss/includes? (.getKey %) ".tpxz") summaries)
        summaries (remove #(contains? seen (.getKey %)) summaries)
        summaries (sort-by #(.getKey %) summaries)]
    (inner-key-partitions (seq summaries))))

(defn run-server
  [port bucket logfile]
  (let [logs (io/writer (io/file logfile) :append true)
        seen (with-open [rdr (io/reader logfile)]
              (into #{} (line-seq rdr)))
        slices (atom (key-partitions bucket seen))
        get-slice (fn get-slice []
                    (let [res (atom nil)]
                      (swap! slices
                        (fn [s]
                          (swap! res (fn [_] (first s)))
                          (rest s)))
                      (map #(.getKey %) @res)))
        handler (fn handler [req]
                  (if (= (:uri req) "/get_job")
                    (let [slice (get-slice)]
                      (doseq [artifact slice]
                        (.write logs (format "%s\n" artifact)))
                      (.flush logs)
                      (if (seq slice)
                        {:status 200
                         :headers {"Content-Type" "application/json"}
                         :body (json/generate-string
                                {:artifacts (get-slice)})}
                        {:status 200
                         :headers {"Content-Type" "application/json"}
                         :body (json/generate-string {:done true})}))
                    {:status 404 :body "File not found."}))]
    (run-jetty handler {:port port})))

(defn -main
  [port bucket logfile]
  (run-server (Integer/parseInt port) bucket logfile))
