(ns co.momomo.appsearch.androidappsapk
  (require [co.momomo.soup :refer :all]
           [co.momomo.crawler :as cr]
           [clojure.core.async :as async]
           [co.momomo.async :refer [gocatch]])
  (import [org.jsoup Jsoup]
          [java.net URL URLEncoder]))

(defn download-url
  [requester job]
  (gocatch
    (let [btn-selector (any-pos
                        (%and (tag "a") (has-class "btn-download1")))
          download-page-url
            (->
              (str "https://androidappsapk.co/download/" (:artifact_name job))
              (cr/req requester :get)
              (async/<!)
              (cr/get-body-or-throw)
              (Jsoup/parse)
              (select-attr "href" btn-selector)
              (first))]
      (->
        (str "https://androidappsapk.co" download-page-url)
        (cr/req requester :get)
        (async/<!)
        (cr/get-body-or-throw)
        (Jsoup/parse)
        (select-attr "href" btn-selector)
        (first)))))
