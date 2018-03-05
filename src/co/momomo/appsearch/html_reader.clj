(ns co.momomo.appsearch.html-reader
  (require [clojure.java.io :as io]
           [clj-http.client :as http])
  (import [java.util Scanner]
          [java.io InputStream File]
          [org.tukaani.xz XZInputStream]))

(defn pages
  [^InputStream zipped-input]
  (map #(str % "</html>")
    (-> zipped-input
      (XZInputStream.)
      (Scanner.)
      (.useDelimiter "</html>")
      (iterator-seq))))

(defn xz-pages-from-url
  [url]
  (prn url)
  (pages (:body (http/get url {:as :stream}))))
