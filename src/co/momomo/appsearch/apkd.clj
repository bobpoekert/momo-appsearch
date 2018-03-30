(ns co.momomo.appsearch.apkd
  (require [co.momomo.soup :refer :all]
           [co.momomo.crawler :as cr]
           [co.momomo.http :as http]
           [co.momomo.appsearch.apkpure :as apkp]
           [clojure.string :as ss]
           [cheshire.core :as json]
           [manifold.deferred :as d])
  (import [org.jsoup Jsoup]
          [java.net URI URLEncoder]
          [org.apache.http.client.utils URLEncodedUtils]
          [org.apache.http NameValuePair]))

(def testv (java.util.concurrent.atomic.AtomicReference.))
(defn dbg-print
  [v]
  (.set testv v)
  (prn v)
  v)

(defn extract-apkd
  [res]
  (-> res
    (:body)
    (Jsoup/parse)
    (select-attr "action"
      (any-pos
        (path
          (has-class "download-box")
          (tag "form"))))
    (first)))

(defn apkd-download-url
  [requester job]
  (d/chain
    (->
      (str "https://apkdownloadforandroid.com/" (:artifact_name job))
      (cr/req requester :get))
    extract-apkd))

(defn apkwin-download-url
  [requester job]
  (d/chain
    (->
      (str "https://apkdownloadforwindows.com/app/" (:artifact_name job) "/")
      (cr/req requester :get))
    extract-apkd))

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
      (when-let [url
                  (->
                    (:body res)
                    (Jsoup/parse)
                    (select-attr "href"
                      (any-pos
                        (%and
                          (tag "a")
                          (has-class "btn-success")
                          (kv-val-contains "href" "/download/"))))
                    (first))]
        (str "https://www.apkfollow.com" url)))))

(defn apkbird-download-url
  [requester job]
  (d/chain
    (->
      (str "https://apkbird.com/en/" (:artifact_name job) "/download")
      (cr/req requester :get))
    (fn [res]
      (let [url (->>
                  res
                  (:body)
                  (re-find #"//dl\.apkbird\.com\/(.*?)\"")
                  (second))]
        (when-not (nil? url)
          (ss/replace
            (format "https://dl.apkbird.com/%s" url)
            "&amp;" "&"))))))

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
        (if (or
              (= outp "https://sq.apkandroid.ru:443")
              (ss/includes? outp "downloadatoz.com"))
          nil outp)))))

(defn lieng-download-url
  [requester job]
  (d/chain
    (http/time-deferred (* (Math/random) 5000))
    (fn [_]
      (->
        (format
          "http://choilieng.com/apk-on-pc/%s"
          (:artifact_name job))
        (cr/req requester :get)))
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
          (first))))))

(defn aapk-download-url
  [requester job]
  (let [btn-selector (any-pos
                      (%and (tag "a") (has-class "btn-download1")))
        download-page-url (str "https://androidappsapk.co/download/" (:artifact_name job) "/")]
    (d/chain
      (->
        download-page-url
        (cr/req requester :get))
      (fn [page]
        (->
          page
          (:body)
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

(defn apkbiz-download-url
  [requester job]
  (d/chain
    (->
      (str "https://apkpure.biz/hi/" (:artifact_name job) "/a/download")
      (cr/req requester :get))
    (fn [page]
      (let [tree (Jsoup/parse (:body page))
            f (first (select-attr tree "data-f" (any-pos (has-class "box-details"))))
            ac (first (select-attr tree "data-f" (any-pos (has-class "downbtn"))))]
        (cr/req "https://apkpure.biz/ax.php" requester :post {:form {"f" f "ac" ac}})))
    (fn [rsp]
      (->
        (:body rsp)
        (Jsoup/parse)
        (select-attr "href" (any-pos (tag "a")))
        (first)))))
