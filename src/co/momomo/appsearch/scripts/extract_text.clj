(ns co.momomo.appsearch.scripts.extract-text
  (:gen-class)
  (require [co.momomo.appsearch.apk :as apk]
           [co.momomo.cereal :as cereal]
           [co.momomo.compress :refer [file-datas]]
           [clojure.java.io :as io]
           [clojure.string :as ss])
  (import [net.dongliu.apk.parser ApkFile AbstractApkFile ByteArrayApkFile]))

(set! *warn-on-reflection* true)

(defn extract-data-text
  [v]
  (if (nil? (:data v))
    [nil []]
    (try
      [
        (:name v)
        (->
          ^bytes (:data v)
          (ByteArrayApkFile.)
          (apk/get-strings))]
      (catch NullPointerException e
        [nil []]))))

(defn ^String sanitize
  [v]
  (ss/replace v "\t" "\0"))

(defn extract-text!
  [outfname apk-fnames]
  (with-open [^java.io.Writer outf (io/writer (io/file outfname))]
    (doseq [[n part] (cereal/parmap apk-fnames
                      (comp
                        (mapcat file-datas)
                        (map extract-data-text)))]
      (doseq [[k v locale] part]
        (.write outf (sanitize n))
        (.write outf "\t")
        (.write outf (sanitize k))
        (.write outf "\t")
        (.write outf (sanitize locale))
        (.write outf "\t")
        (.write outf (sanitize v))
        (.write outf "\n")))))

(defn directory?
  [^java.io.File f]
  (.isDirectory f))

(defn -main
  [inp-dirname outfname]
  (extract-text! outfname (remove directory? (file-seq (io/file inp-dirname)))))
