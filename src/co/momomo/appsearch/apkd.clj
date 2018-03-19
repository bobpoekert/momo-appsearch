(ns co.momomo.appsearch.apkd
  (require [co.momomo.soup :refer :all]
           [co.momomo.crawler :as cr]
           [co.momomo.appsearch.apkpure :as apkp]
           [clojure.string :as ss]
           [cheshire.core :as json]
           [manifold.deferred :as d])
  (import [org.jsoup Jsoup]
          [java.net URI URLEncoder]
          [org.apache.http.client.utils URLEncodedUtils]
          [org.apache.http NameValuePair]))

(defn apkd-download-url
  [requester job]
  (d/chain
    (->
      (str "https://apkdownloadforandroid.com/" (:artifact_name job))
      (cr/req requester :get))
    (fn [res]
      (-> res
        (:body)
        (Jsoup/parse)
        (select-attr "href"
          (any-pos
            (%and
              (tag "a")
              (kv "id" "btn-download"))))
        (first)))))

(defn apkname-download-url
  [requester job]
  (d/chain
    (->
      (str "https://apkname.com/download/" (:artifact_name job))
      (cr/req requester :get))
    (fn [res]
      (->
        (->>
          (:body res)
          (re-find #"get_files\(['\"]?(.*?)['\"]")
          (second)
          (format "https://apkname.com/apk.php?com=%s&server=1"))
        (cr/req requester :get)))
    (fn [res]
      (->
        (:body res)
        (Jsoup/parse)
        (select-attr "src" (any-pos (tag "iframe")))
        (first)))))

(defn apkfollow-download-url
  [requester job]
  (d/chain
    (->
      (str "https://www.apkfollow.com/app/task-management/"
        (:artifact_name job) "/")
      (cr/req requester :get))
    (fn [res]
      (str "https://www.apkfollow.com"
        (->
          (:body res)
          (Jsoup/parse)
          (select-attr "href"
            (any-pos
              (%and
                (tag "a")
                (has-class "btn-success")
                (kv-val-contains "href" "/download/"))))
          (first))))))

(defn apkbird-download-url
  [requester job]
  (d/chain
    (->
      (str "https://apkbird.com/en/" (:artifact_name job) "/download")
      (cr/req requester :get))
    (fn [res]
      (->
        (->>
          res
          (:body)
          (re-find #"//dl\.apkbird\.com\/(.*?)\"")
          (second)
          (format "https://dl.apkbird.com/%s"))
        (ss/replace "&amp;" "&")))))

(defn apkdl-download-url
  [requester job]
  (d/chain
    (->
      (str "https://apkdownloadforandroid.com/" (:artifact_name job) "/")
      (cr/req requester :get))
    (fn [res]
      (->
        (:body res)
        (Jsoup/parse)
        (select-attr "href"
          (any-pos
            (path
              (%and (tag "div") (has-class "download-box"))
              (%and (tag "a") (kv "id" "btn-download")))))
        (first)))))

(defn apkdroid-download-url
  [requester job]
  (d/chain
    (->
      (str "https://www.apkandroid.ru/a/"
        (:artifact_name job) "/downloading.html")
      (cr/req requester :get))
    (fn [res]
      (->
        (->>
          (:body res)
          (re-find #"\"https://www.apkandroid.ru/api/api\.php\",\{id\:\"(.*?)\"")
          (second)
          (format "https://www.apkandroid.ru/api/api.php?id=%s"))
        (cr/req requester :get)))
    (fn [res]
      (let [outp
              (->
                (:body res)
                (json/parse-string)
                (get "url"))]
        (if (ss/includes? outp "downloadatoz.com") nil outp)))))

(defn lieng-download-url
  [requester job]
  (d/chain
    (->
      (str 
        "http://choilieng.com/apk-on-pc/'http://choilieng.com/apk-on-pc/"
        (:artifact_name job) "?PageSpeed=noscript%27")
      (cr/req requester :get))
    (fn [page]
      (->>
        (->
          (:body page)
          (Jsoup/parse)
          (select-attr "href"
            (any-pos
              (%and
                (tag "a")
                (has-class "top-download")
                (kv "itemprop" "downloadUrl"))))
          ^String (first)
          (URI.)
          (URLEncodedUtils/parse "UTF-8"))
        (map (fn [^NameValuePair kv]
              (if (= (.getName kv) "appid")
                (str "appid=" (:artifact_name job) "_appnaz.com_")
                (str (.getName kv) "=" (.getValue kv)))))
        (ss/join "&")
        (str "http://choilieng.com/download?")))))

(defn aapk-download-url
  [requester job]
  (let [btn-selector (any-pos
                      (%and (tag "a") (has-class "btn-download1")))
        download-page-url (str "https://androidappsapk.co/download/" (:artifact_name job))]
    (d/chain
      (->
        download-page-url
        (cr/req requester :get))
      (fn [page]
        (->
          (:body page)
          (Jsoup/parse)
          (select-attr "href" btn-selector)
          (first)
          (str "https://androidappsapk.co" download-page-url)
          (cr/req requester :get)))
      (fn [page]
        (->
          (:body page)
          (Jsoup/parse)
          (select-attr "href" btn-selector)
          (first))))))

(defn apkp-download-url
  [requester job]
  (d/chain
    (->
      (str "https://apkpure.com" (:download_url job))
      (cr/req requester :get))
    (fn [page]
      (apkp/extract-download-url (:body page)))))
