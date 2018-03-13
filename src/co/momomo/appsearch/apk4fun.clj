(ns co.momomo.appsearch.apk4fun
  (require [co.momomo.soup :refer :all]
           [clojure.string :as ss]
           [clojure.java.io :as io]
           [clj-http.client :as http]
           [cheshire.core :as json])
  (import [org.jsoup Jsoup]
          [java.net URL URLEncoder]))

(defn info-page-url
  [^String artifact-name]
  (str "https://www.apk4fun.com/apps/" artifact-name "/"))

(def rapidgator-key
  (delay
    (with-open [ins (io/reader (io/resource "rapidgator_creds"))]
      (let [lines (line-seq ins)
            username (first lines)
            pass (second lines)]
        (->
          (http/post "https://rapidgator.net/api/user/login"
            {:form-params {:username username :password pass}
             :as :json})
          (:body)
          (:response)
          (:session_id))))))

(defn rapidgator-url
  ([^String raw-url opts]
    (->
      (http/get
        (str "https://rapidgator.net/api/file/download?sid="
          @rapidgator-key
          "&url="
          (URLEncoder/encode raw-url "UTF-8"))
        (merge opts {:as :json}))
      (:body)
      (:response)
      (:url)))
  ([raw-url]
    (rapidgator-url raw-url {})))

(defn app-meta
  [tree]
  {:thumbnail_url (first
                    (select-attr tree "src"
                      (any-pos
                        (path
                          (%and (tag "div") (has-class "post-icon"))
                          (tag "img")))))
   :apk4fun_id (->>
                  (select-attr tree "href" (tag "a"))
                  (map #(re-find #"/apk/(\d+)/?" %))
                  (filter #(> (count %) 1))
                  (first)
                  (second))
  :description (->>
                (any-pos
                  (path
                    (%and
                      (tag "div")
                      (has-class "post-content")
                      (has-class "entry_content"))
                    (tag "article")
                    (tag "p")))
                (select tree)
                (map get-text))})

(defn artifacts-meta
  [tree]
  (filter :file_urls
    (for [artifact (select tree (any-pos (kv-val-regex "id" #"apk-\d+")))]
      (assoc
        (into {}
          (->>
            (select artifact (tag "p"))
            (map get-text)
            (filter (fn [^String v] (.startsWith v "• ")))
            (map
              (fn [row]
                (let [[_ ^String k ^String v] (re-find #"• (.+?):(.*)" row)]
                  {(.trim k) (.trim v)})))))
        :file_urls (->>
                    (any-pos
                      (%and (tag "a")
                            (kv-val-regex "title" #"^Download APK from")))
                    (select artifact)
                    (map (fn [v] {(.toLowerCase (get-text v))
                                  (getattr v "href")}))
                    (reduce merge))))))

(defn get-artifacts-meta
  [id]
  (->
    (http/get (str "https://www.apk4fun.com/apk/" id "/"))
    (:body)
    (Jsoup/parse)
    (artifacts-meta)))

(defn artifact-download-url
  [artifact-name]
  (try
    (let [id (->
              (http/get (info-page-url artifact-name))
              (:body)
              (Jsoup/parse)
              (app-meta)
              (:apk4fun_id))
          filter2 (fn [a b] (filter b a))
          artifacts (get-artifacts-meta id)
          rapid-url (->>
                      artifacts
                      (map #(get-in % [:file_urls "rapidgator"]))
                      (filter (complement nil?))
                      (first))
          rapid-url (->
                      (str "https://apk4fun.com" rapid-url)
                      (http/get)
                      (:body)
                      (Jsoup/parse)
                      (select-attr "href"
                        (any-pos
                          (%and (tag "a")
                                (kv "title" "Download from Rapidgator"))))
                      (first)
                      (URL.)
                      (.getQuery)
                      (ss/split #"&")
                      (filter2 (fn [^String v] (.startsWith v "l=")))
                      (first)
                      (ss/split #"=")
                      ^String (second)
                      (java.net.URLDecoder/decode))]
      (rapidgator-url rapid-url))
    (catch Exception e nil)))
