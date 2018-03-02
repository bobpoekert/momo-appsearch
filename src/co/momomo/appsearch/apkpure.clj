(ns co.momomo.appsearch.apkpure
  (require [clojure.string :as ss]
           [co.momomo.soup :refer :all])
  (import [org.jsoup Jsoup]
          [org.jsoup.nodes Element]))


(set! *warn-on-reflection* true)

(defn select-strip
  [tree selection]
  (if-let [^Element v (first (select tree selection))]
    (.trim (.text v))))

(defn getattr
  [^Element e ^String k]
  (if e
    (.get (.attributes e) k)))

(defn select-attr
  [tree attr selection]
  (->>
    (select tree selection)
    (map #(getattr % attr))))

(defn parse-infobox-row
  [^Element row]
  (let [k (select-strip row (tag "strong"))
        v (-> row (.parent) (.text))]
    {k v}))

(defn app-meta
  [tree]
  (let [infobox (select tree (path (id "fax_box_faq2") (tag "dl") (tag "dd") (tag "p")))
        infobox (if infobox (first infobox))
        title-box (first (select tree 
                          (any-pos
                            (path
                              (%and (tag "div") (has-class "box"))
                              (%and (tag "dl") (has-class "ny-dl"))))))
        category-a (first (select tree
                    (any-pos
                      (->
                        (path
                          (%and (tag "div") (has-class "additional"))
                          (tag "ul")
                          (tag "li")
                          (tag "p")
                          (%and (tag "strong") (contains-own-text "Category:")))
                        (parent)
                        (parent)
                        (path
                          (tag "p")
                          (nth-of-type 2)
                          (tag "a"))))))
        thumbnail-selector (any-path
                            (path
                              (%and (tag "div") (has-class "describe"))
                              (%and (tag "div") (has-class "describe-img")))
                            (%and (tag "a") (kv "target" "_blank")))]
    (merge
      (reduce merge (map parse-infobox-row infobox))
      {:thumbnail_url 
        (select-strip title-box (path
                                  (%and (tag "div") (has-class "icon"))
                                  (tag "img")
                                  (has-attr "src")))
      :title (select-strip title-box
              (path
                (%and (tag "div") (has-class "title-like"))
                (tag "h1")))
      :author_name (select-strip tree
                    (any-path
                      (kv "itemtype" "http://schema.org/Organization")
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
                      (map get-text)
                      (ss/join "\n"))
                    (catch Exception e
                      (select-strip tree
                        (any-pos
                          (path
                            (id "describe")
                            (%and (tag "div") (has-class "description"))
                            (%and (tag "div") (has-class "content")))))))
      :category_href (getattr category-a "href")
      :category_tags (map get-text (select category-a (tag "span")))
      :publish_date (select-strip tree
                      (->
                        (any-path
                          (%and (tag "div") (has-class "additional"))
                          (path
                            (tag "p")
                            (%and (tag "strong") (contains-own-text "Publish Date:"))))
                        (parent)
                        (parent)
                        (path
                          (tag "p")
                          (nth-of-type 2))))
      :appstore_links (->>
                        (select tree
                          (any-pos
                            (%and (tag "a") (kv-val-contains "ga" "get_it_on"))))
                        (map #(getattr % "href")))
      :requirements (->>
                      (any-pos (parent (parent (contains-own-text "Requirements:"))))
                      (select tree)
                      (map get-text))
      :version_urls (select-attr tree "href"
                      (path
                        (%and (tag "div") (has-class "version"))
                        (%and (tag "ul") (has-class "ver-wrap"))
                        (tag "li")
                        (tag "a")))
      :snapshot_image_urls_800 (select-attr tree "href" thumbnail-selector)
      :screenshot_image_urls_350 (select-attr tree "src" thumbnail-selector)})))
