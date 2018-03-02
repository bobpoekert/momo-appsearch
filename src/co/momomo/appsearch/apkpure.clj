(ns co.momomo.appsearch.apkpure
  (require [clojure.string :as ss]
           [co.momomo.soup :refer :all])
  (import [org.jsoup Jsoup Element]))

(defn select-strip
  [tree selection]
  (->
    (select tree selection)
    (first)
    (.text)
    (.trim)))

(defn getattr
  [^Element e ^String k]
  (.get (.attributes e) k))

(defn select-attr
  [tree attr selection]
  (->>
    (select tree selection)
    (map #(getattr % attr))))

(defn parse-infobox-row
  [row]
  (let [k (select-strip row (tag "strong"))
        v (-> row (.parent) (.text))]
    {k v}))

(defn app-meta
  [page]
  (let [tree (Jsoup/parse page)
        infobox (select tree (path (id "fax_box_faq2") (tag "dl") (tag "dd") (tag "p")))
        infobox (if infobox (first infobox))
        title-box (first (select tree 
                          (path
                            (wildcard)
                            (%and (tag "div") (class-contains "box"))
                            (%and (tag "dl") (class-contains "ny-dl")))))
        category-a (first (select tree
                    (->
                      (path
                        (wildcard)
                        (%and (tag "div") (contains-class "additional"))
                        (tag "ul")
                        (tag "li")
                        (tag "p")
                        (%and (tag "strong") (contains-own-text "Category:")))
                      (parent)
                      (parent)
                      (path
                        (%nth (tag "p") 2)
                        (tag "a")))))
        thumbnail-selector (path
                            (wildcard)
                            (%and (tag "div") (contains-class "describe"))
                            (%and (tag "div") (contains-class "describe-img"))
                            (wildcard)
                            (%and (tag "a") (kv "target" "_blank")))]
    (merge
      (reduce merge (map parse-infobox-row infobox))
      {:thumbnail_url 
        (select-strip title-box (path
                                  (%and (tag "div") (contains-class "icon"))
                                  (tag "img")
                                  (has-attr "src")))
      :title (select-strip title-box
              (path
                (%and (tag "div") (contains-path "title-like"))
                (tag "h1")))
      :author_name (select-strip tree
                    (path
                      (kv "itemtype" "http://schema.org/Organization")
                      (wildcard)
                      (tag "span")))
      :author_url (select-strip tree
                    (path
                      (kv "itemtype" "http://schema.org/Organization")
                      (tag "a")))
      :description (try
                    (->>
                      (select tree
                        (path
                          (id "describe")
                          (tag "div")
                          (tag "div")))
                      (map #(.text %))
                      (ss/join "\n"))
                    (catch Exception e
                      (select-strip tree
                        (path
                          (wildcard)
                          (%and (tag "div") (id "describe"))
                          (%and (tag "div") (class-contains "description"))
                          (%and (tag "div") (class-contains "content"))))))
      :category_href (getattr category-a "href")
      :category_tags (select-strip category-a (span "tag"))
      :publish_date (select-strip tree
                      (path
                        (wildcard)
                        (%and (tag "div") (contains-class "additional"))
                        (wildcard)
                        (tag "p")
                        (%and (tag "strong") (contains-own-text "Publish Date:"))
                        (parent)
                        (parent)
                        (%nth (tag "p") 2)))
      :appstore_links (->
                        (select tree
                          (path
                            (wildcard)
                            (%and (tag "a") (attr-v-contains "ga" "get_it_on"))))
                        (map #(getattr % "href")))
      :requirements (select-attr tree "href"
                      (path
                        (wildcard)
                        (%and (tag "div") (contains-class "version"))
                        (%and (tag "ul") (contains-class "ver-wrap"))
                        (tag "li")
                        (tag "a")))
      :version_urls (select-attr tree "href"
                      (path
                        (%and (tag "div") (contains-class "version"))
                        (%and (tag "ul") (contains-class "ver-wrap"))
                        (tag "li")
                        (tag "a")))
      :snapshot_image_urls_800 (select-attr tree "href" thumbnail-selector)
      :screenshot_image_urls_350 (select-attr tree "src" thumbnail-selector)})))
