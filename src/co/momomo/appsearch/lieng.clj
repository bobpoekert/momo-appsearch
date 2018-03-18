(ns co.momomo.appsearch.lieng
  (require [co.momomo.soup :refer :all]
           [co.momomo.crawler :as cr]
           [clojure.string :as ss]
           [clj-http.core :as hc]
           [clj-http.cookies :as cookies]
           [clojure.core.async :as async]
           [co.momomo.async :refer [gocatch]])
  (import [org.jsoup Jsoup]
          [java.net URI URLEncoder]
          [org.apache.http.client.utils URLEncodedUtils]
          [org.apache.http NameValuePair]))

(defn download-url
  [requester job]
  (gocatch
    (->>
      (->
        (str 
          "http://choilieng.com/apk-on-pc/'http://choilieng.com/apk-on-pc/"
          (:artifact_name job) "?PageSpeed=noscript%27")
        (cr/req requester :get)
        (async/<!)
        (cr/get-body-or-throw)
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
      (str "http://choilieng.com/download?"))))
