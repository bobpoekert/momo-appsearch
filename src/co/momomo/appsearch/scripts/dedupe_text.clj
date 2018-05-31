(ns co.momomo.appsearch.scripts.dedupe-text
  (require [clojure.string :as ss]
           [clojure.java.io :as io])
  (import [gnu.trove.set.hash TIntHashSet]))

(defn -main
  [infname outfname]
  (with-open [^java.io.BufferedReader inf (java.io.BufferedReader. (io/reader (io/file infname)))
              ^java.io.Writer outf (io/writer (io/file outfname))]
    (let [hashes (TIntHashSet.)]
      (loop [current-hash nil
             repeat? false]
        (let [^String row (str (.readLine inf) "\n")]
          (when-not (nil? row)
            (let [app-name (first (ss/split row #"\t" 1))
                  app-hash (hash app-name)]
              (if (= app-hash current-hash)
                (when-not repeat? (.write outf row))
                (let [repeat? (.contains hashes app-hash)]
                  (when-not repeat?
                    (.add hashes app-hash)
                    (.write outf row))
                  (recur app-hash repeat?))))))))))
           
