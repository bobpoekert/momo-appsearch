(ns co.momomo.appsearch.lieng
  (require [co.momomo.soup :refer :all]
           [co.momomo.crawler :as cr]
           [clojure.string :as ss]
           [clj-http.core :as hc]
           [clj-http.cookies :as cookies])
  (import [org.jsoup Jsoup]
          [java.net URI URLEncoder]
          [org.apache.http.client.utils URLEncodedUtils]
          [org.apache.http NameValuePair]))

(defn download-url
  [artifact-name]
  (->>
    (->
      (str 
        "http://choilieng.com/apk-on-pc/'http://choilieng.com/apk-on-pc/"
        artifact-name "?PageSpeed=noscript%27")
      (cr/req :get)
      (:body)
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
            (str "appid=" artifact-name "_appnaz.com_")
            (str (.getName kv) "=" (.getValue kv)))))
    (ss/join "&")
    (str "http://choilieng.com/download?")))
