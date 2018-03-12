(ns co.momomo.appsearch.scripts.apkpure-aws
  (:gen-class)
  (require [co.momomo.appsearch.apkpure :as apkp]))

(defn -main
  [& args]
  (apply apkp/download-and-process-apps-s3! args))
    
