(ns co.momomo.appsearch.androidappsapk
  (require [co.momomo.soup :refer :all]
           [co.momomo.crawler :as cr]
           [clj-http.core :as hc]
           [clj-http.cookies :as cookies])
  (import [org.jsoup Jsoup]
          [java.net URL URLEncoder]))

(defn download-url
  [artifact-name]
  (let [btn-selector (any-pos
                      (%and (tag "a") (has-class "btn-download1")))
        download-page-url
          (->
            (str "https://androidappsapk.co/download/" artifact-name)
            (cr/req :get)
            (:body)
            (Jsoup/parse)
            (select-attr "href" btn-selector)
            (first))]
    (->
      (str "https://androidappsapk.co" download-page-url)
      (cr/req :get)
      (:body)
      (Jsoup/parse)
      (select-attr "href" btn-selector)
      (first))))
