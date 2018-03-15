(ns co.momomo.appsearch.apkd
  (require [co.momomo.soup :refer :all]
           [co.momomo.crawler :as cr]
           [clojure.string :as ss])
  (import [org.jsoup Jsoup]))

(defn apkd-download-url
  [artifact-name]
  (->
    (str "https://apkdownloadforandroid.com/" artifact-name)
    (cr/req :get)
    (:body)
    (Jsoup/parse)
    (select-attr "href"
      (any-pos
        (%and
          (tag "a")
          (kv "id" "btn-download"))))
    (first)))

(defn apkname-download-url
  [artifact-name]
  (->
    (->>
      (->
        (str "https://apkname.com/download/" artifact-name)
        (cr/req :get))
      (:body)
      (re-find #"get_files\(['\"]?(.*?)['\"]")
      (second)
      (format "https://apkname.com/apk.php?com=%s&server=1"))
    (cr/req :get)
    (:body)
    (Jsoup/parse)
    (select-attr "src" (any-pos (tag "iframe")))
    (first)))
