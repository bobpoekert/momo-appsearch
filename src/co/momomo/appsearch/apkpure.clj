(ns co.momomo.appsearch.apkpure
  (require [clojure.string :as ss]
           [co.momomo.soup :refer :all])
  (import [org.jsoup Jsoup]
          [org.jsoup.nodes Element]
          [com.joestelmach.natty Parser DateGroup]))

(set! *warn-on-reflection* true)

(defn parse-date
  [^String d]
  (for [^DateGroup g (.parse (Parser.) d)
        date (.getDates g)]
    date))


(defn parse-infobox-row
  [^Element row]
  (let [k (select-strip row (tag "strong"))
        v (-> row (.parent) (.text))]
    {k v}))

(defn parse-app-version-meta
  [^String k ^String v]
  (let [k (.toLowerCase (.trim k))]
    (case k
      "signature:" {:signature (.trim v)}
      "file sha1:" {:file_sha1 (.trim v)}
      "update on:" {:updated_on (first (parse-date v))}
      "requires android:" {:android_version (.trim v)}
      "file size:" {:file_size (.trim v)}
      :else {k v})))

(defn extract-app-versions
  [tree]
  (for [version (select tree (path (%and (tag "ul") (has-class "ver-wrap")) (tag "li")))]
    (apply merge
      {:url (first (select-attr version "href" (tag "a")))}
      (for [^Element row (select version (path (has-class "ver-info-m") (tag "p")))]
        (if (empty? (.getElementsByTag row "a"))
          (parse-app-version-meta
            (.text ^Element (first (.getElementsByTag row "strong")))
            (.trim (.ownText row)))
          {})))))

(def category-a 
  (->
    (any-pos (has-class "additional"))
    (parent)
    (parent)
    (any-path (%and (tag "a") (kv-val-contains "title" "Download more")))))

(def thumbnail-selector 
  (any-path
    (path
      (%and (tag "div") (has-class "describe"))
      (%and (tag "div") (has-class "describe-img")))
    (%and (tag "a") (kv "target" "_blank"))))

(defn app-meta
  [tree]
  (let [infobox (select tree (path (id "fax_box_faq2") (tag "dl") (tag "dd") (tag "p")))
        infobox (if infobox (first infobox))
        title-box (first (select tree 
                          (any-pos
                            (path
                              (%and (tag "div") (has-class "box"))
                              (%and (tag "dl") (has-class "ny-dl"))))))]
      {:thumbnail_url (first (select-attr tree "src"
                        (path
                          (%and (tag "div") (has-class "icon"))
                          (tag "img"))))
      :title (select-strip title-box
              (path
                (%and (tag "div") (has-class "title-like"))
                (tag "h1")))
      :author_name (select-strip tree
                    (any-path
                      (kv "itemtype" "http://schema.org/Organization")
                      (tag "span")))
      :author_url (first
                    (select-attr tree "href"
                      (path
                        (kv "itemtype" "http://schema.org/Organization")
                        (tag "a"))))
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
      :category_url (first (select-attr tree "href" category-a))
      :category_tags (map get-text (select tree (path category-a (tag "span"))))
      :appstore_links (->>
                        (select tree
                          (any-pos
                            (%and (tag "a") (kv-val-contains "ga" "get_it_on"))))
                        (map #(getattr % "href")))
      :versions (extract-app-versions tree)
      :snapshot_image_urls_800 (select-attr tree "href" thumbnail-selector)
      :screenshot_image_urls_350 (select-attr tree "src" (path thumbnail-selector (tag "img")))}))
