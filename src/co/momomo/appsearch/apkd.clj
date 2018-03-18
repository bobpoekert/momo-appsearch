(ns co.momomo.appsearch.apkd
  (require [co.momomo.soup :refer :all]
           [co.momomo.crawler :as cr]
           [clojure.string :as ss]
           [cheshire.core :as json]
           [clojure.core.async :as async]
           [co.momomo.async :refer [gocatch]])
  (import [org.jsoup Jsoup]))

(defn apkd-download-url
  [requester job]
  (gocatch
    (->
      (str "https://apkdownloadforandroid.com/" (:artifact_name job))
      (cr/req requester :get)
      (async/<!)
      (cr/get-body-or-throw)
      (Jsoup/parse)
      (select-attr "href"
        (any-pos
          (%and
            (tag "a")
            (kv "id" "btn-download"))))
      (first))))

(defn apkname-download-url
  [requester job]
  (gocatch
    (->
      (->>
        (->
          (str "https://apkname.com/download/" (:artifact_name job))
          (cr/req :get))
        (cr/get-body-or-throw)
        (re-find #"get_files\(['\"]?(.*?)['\"]")
        (second)
        (format "https://apkname.com/apk.php?com=%s&server=1"))
      (cr/req requester :get)
      (async/<!)
      (cr/get-body-or-throw)
      (Jsoup/parse)
      (select-attr "src" (any-pos (tag "iframe")))
      (first))))

(defn apkfollow-download-url
  [requester job]
  (gocatch
    (str "https://www.apkfollow.com"
      (->
        (str "https://www.apkfollow.com/app/task-management/" (:artifact_name job) "/")
        (cr/req requester :get)
        (async/<!)
        (cr/get-body-or-throw)
        (Jsoup/parse)
        (select-attr "href"
          (any-pos
            (%and
              (tag "a")
              (has-class "btn-success")
              (kv-val-contains "href" "/download/"))))
        (first)))))

(defn apkbird-download-url
  [requester job]
  (gocatch
    (->
      (->>
        (->
          (str "https://apkbird.com/en/" (:artifact_name job) "/download")
          (cr/req requester :get)
          (async/<!)
          (cr/get-body-or-throw))
        (re-find #"//dl\.apkbird\.com\/(.*?)\"")
        (second)
        (format "https://dl.apkbird.com/%s"))
      (ss/replace "&amp;" "&"))))

(defn apkdl-download-url
  [requester job]
  (gocatch
    (->
      (str "https://apkdownloadforandroid.com/" (:artifact_name job) "/")
      (cr/req requester :get)
      (async/<!)
      (cr/get-body-or-throw)
      (Jsoup/parse)
      (select-attr "href"
        (any-pos
          (path
            (%and (tag "div") (has-class "download-box"))
            (%and (tag "a") (kv "id" "btn-download")))))
      (first))))

(defn apkdroid-download-url
  [requester job]
  (gocatch
    (let [res
      (->
        (->>
          (->
            (str "https://www.apkandroid.ru/a/" (:artifact_name job) "/downloading.html")
            (cr/req :get)
            (cr/get-body-or-throw))
          (re-find #"\"https://www.apkandroid.ru/api/api\.php\",\{id\:\"(.*?)\"")
          (second)
          (format "https://www.apkandroid.ru/api/api.php?id=%s"))
        (cr/req requester :get)
        (async/<!)
        (cr/get-body-or-throw)
        (json/parse-string)
        (get "url"))]
      (if (ss/includes? res "downloadatoz.com") nil res))))
