(ns co.momomo.appsearch.html-reader
  (require [clojure.java.io :as io]
           [clj-http.client :as http]
           [co.momomo.compress :refer [xz-input-stream]])
  (import [java.util Scanner]
          [java.io InputStream File]))

(defn pages
  [^InputStream zipped-input]
  (map #(str % "</html>")
    (-> zipped-input
      (xz-input-stream)
      (Scanner.)
      (.useDelimiter "</html>")
      (iterator-seq))))

(defn xz-pages-from-url
  [url]
  (pages (:body (http/get url {:as :stream}))))
