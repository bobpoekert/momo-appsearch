(ns co.momomo.appsearch.scripts.apkpure-extract
  (:gen-class)
  (require [co.momomo.appsearch.apkpure :as apkp]))

(defn -main
  [outf & args]
  (apkp/parse-pages! (java.io.File. outf)))
