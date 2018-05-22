(ns co.momomo.appsearch.scripts.extract-text
  (:gen-class)
  (require [co.momomo.appsearch.apk :as apk]
           [co.momomo.cereal :as cereal]
           [co.momomo.compress :refer [file-datas]]
           [clojure.java.io :as io])
  (import [net.dongliu.apk.parser ApkFile AbstractApkFile ByteArrayApkFile]))

(set! *warn-on-reflection* true)

(defn extract-data-text
  [v]
  (->
    ^bytes (:data v)
    (ByteArrayApkFile.)
    (apk/extract-text)))

(defn extract-text!
  [outfname apk-fnames]
  (with-open [^java.io.Writer outf (io/writer (io/file outfname))]
    (doseq [part (cereal/parmap apk-fnames (map (comp extract-data-text (mapcat file-datas))))
            [k v locale] part]
      (.write outf k)
      (.write outf v)
      (.write outf locale)
      (.write outf "\n"))))

(defn directory?
  [^java.io.File f]
  (.isDirectory f))

(defn -main
  [inp-dirname outfname]
  (extract-text! outfname (remove directory? (file-seq (io/file inp-dirname)))))
