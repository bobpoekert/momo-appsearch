(ns co.momomo.crawler
  (require [co.momomo.cereal :as cereal]
           [co.momomo.soup :as soup]
           [clojure.java.io :as io]
           [clojure.string :as ss]
           [manifold.deferred :as d]
           [co.momomo.http :as http])
  (import [java.util.concurrent LinkedBlockingQueue TimeUnit]
          [java.net Socket InetSocketAddress]
          [java.util ArrayDeque HashMap]
          [com.google.common.io BaseEncoding]
          [org.jsoup Jsoup]))

(set! *warn-on-reflection* true)

(defn proxy-hosts
  [list-url]
  (d/chain
    (http/req list-url :get)
    (fn [rsp]
      (->> rsp
        (:body)
        (ss/split-lines)
        (filter (fn [^String v] (> (.indexOf v ":") -1)))
        (map (fn [^String v]
              (let [v (ss/replace v #"\</?pre\>" "")
                    [ip port] (ss/split v #":")]
                [ip (Integer/parseInt port)])))
        (vec)))))

(defn ^String b64decode
  [^String v]
  (->
    (BaseEncoding/base64)
    (.decode v)))

(defn proxycz-hosts
  [rrs page-number]
  (d/chain
    (http/req-retry (str "http://free-proxy.cz/en/proxylist/main/" page-number) rrs :get)
    (fn [page]
      (let [root (second (re-find #"\<table id=\"proxy_list\">(.*)\</table\>" (:body page)))]
        (for [row (re-seq #"\<tr\>(.*?)\</tr\>" root)]
          (let [match (second row) 
                b64-ip (second (re-find #"document\.write\(Base64\.decode\(\"(.*?)\"\)\)" match))
                ip (.trim (b64decode b64-ip))
                port (->
                      (re-find #"\<span class=\"fport\" style=\'\'\>(\d+)\</span\>" match)
                      (second)
                      (Integer/parseInt))
                protocol (cond
                          (ss/includes? match "SOCKS4") :socks4
                          (ss/includes? match "SOCKS5") :socks5
                          (ss/includes? match "HTTPS") :http
                          (ss/includes? match "HTTP") :http)]
              [protocol ip port]))))))

(defn proxycz-proxies
  [rrs]
  (d/chain
    (apply d/zip
      (map (partial proxycz-hosts rrs) (range 15)))
    #(apply concat %)))

(defn filefab-proxies
  []
  (d/let-flow [http (proxy-hosts "http://filefab.com/api.php?l=Od9bhDRsZz5XCq2_waX09Wcll-DtXOXvHJNe68cZblE")
               socks4 (proxy-hosts "http://filefab.com/api.php?l=jcW8aUaS9GCCqiDQDS9tu7RU8CCsNVthZuWPAU2dzcI")
               socks5 (proxy-hosts "http://filefab.com/api.php?l=P9TdKt1_-xtqwtlCXVASfpGQ6-i3cxYgvTD8Bs2HUSU")]
    (concat
      (map (fn [[h p]] [:http h p]) http)
      (map (fn [[h p]] [:socks4 h p]) socks4)
      (map (fn [[h p]] [:socks5 h p]) socks5))))

(defn spys-proxies
  [requesters params]
  (d/chain
    (http/req-retry "http://spys.one/en/socks-proxy-list/" requesters :post
      {:form params})
    (fn [rsp]
      (prn (:status rsp))
      (let [tree (Jsoup/parse (:body rsp))
            consts-js (second (re-find #"<script type=\"text/javascript\">(.*?)</script>" (:body rsp)))
            xor-eval (fn [consts expr]
                      (reduce
                        (fn [acc elem]
                          (bit-xor acc
                            (if (contains? consts elem)
                              (get consts elem)
                              (Integer/parseInt elem))))
                        0 (ss/split expr #"\^")))
            consts (reduce
                      (fn [acc line]
                        (if (ss/blank? line)
                          acc
                          (let [[k v] (ss/split line #"=" 2)]
                            (assoc acc k (xor-eval acc v)))))
                      {} (ss/split consts-js #";"))
            parse-port (fn [port-string]
                        (->>
                          (->
                            (re-find #"document\.write\(\".*?\"\+(.*)\)" port-string)
                            (second)
                            (ss/split #"\+"))
                          (filter #(> (.length %) 0))
                          (map
                            (fn [expr]
                              (->>
                                (ss/replace expr #"[\(\)]" "")
                                (xor-eval consts))))
                          (reduce
                            (fn [res digit]
                              (+ (* res 10) digit))
                            0)))
            rows (soup/select tree
                  (soup/any-pos
                    (soup/%and
                      (soup/tag "tr")
                      (soup/%or
                        (soup/has-class "spy1x")
                        (soup/has-class "spy1xx"))
                      (soup/has-attr "onmouseover"))))]
        (filter identity
          (for [row rows]
            (let [cols (soup/select row
                        (soup/%and
                          (soup/tag "td")
                          (soup/kv "colspan" "1")))]
              (if (< (count cols) 2)
                nil
                (try
                  (let [host (second (ss/split (.text (first cols)) #"\s+"))
                        protocol (.trim (soup/get-text (second cols)))
                        port-js (second (re-find #"<script type=\"text/javascript\">(.*?)</script>" (.outerHtml row)))
                        port (parse-port port-js)]
                    [
                      (cond
                        (ss/includes? protocol "SOCKS5") :socks5
                        (ss/includes? protocol "SOCKS4") :socks4
                        (ss/includes? protocol "HTTP") :http)
                      host port])
                  (catch Exception e
                    (do
                      (prn e)
                      nil)))))))))))

(defn local-proxy-list
  []
  (with-open [ins (io/reader (io/resource "vipsocks.txt"))]
    (->
      (for [row (line-seq ins)]
        (let [[k v] (ss/split row #":")]
          [:socks5 k (Integer/parseInt (.trim v))]))
      (doall))))

(defn get-proxies
  []
  (let [pl (into #{} (local-proxy-list))
        local-rr (map #(apply http/make-requester %) pl)]
    (d/chain
      (d/zip
        (filefab-proxies)
       ; (proxycz-proxies local-rr)
        (spys-proxies local-rr {"xf1" "0" "xf2" "0" "xf4" "0" "xf5" "0" "xpp" "5"})
        (spys-proxies local-rr {"xf1" "0" "xf2" "0" "xf4" "0" "xf5" "1" "xpp" "5"})
        (spys-proxies local-rr {"xf1" "0" "xf2" "0" "xf4" "0" "xf5" "2" "xpp" "5"}))
      (fn [results]
        (->> results
          (apply concat)
          (remove #(contains? pl %))
          (into #{})
          (map #(apply http/make-requester %))
          (concat local-rr))))))

(def req http/req)

(defn make-requester
  [thunk proxy-protocol proxy-host proxy-port]
  (->
    (http/make-requester proxy-protocol proxy-host proxy-port)
    (assoc :thunk thunk)))

(defn requesters
  [requester-fns]
  (let [proxies @(get-proxies)]
    (for [thunk requester-fns
          px proxies]
      (assoc px :thunk thunk))))


(defn crawl
  [inp opts requester-fns]
  (let [^ArrayDeque empties (ArrayDeque. ^java.util.Collection (requesters requester-fns))
        ^LinkedBlockingQueue results (LinkedBlockingQueue. 20)]
    (prn "start")
    (loop [inp inp
           last-refresh (System/currentTimeMillis)]
      (cond
        (or (< (.size empties) 200) (< (let [r (Runtime/getRuntime)] (- (.maxMemory r) (- (.totalMemory r) (.freeMemory r)))) (* 1000 1000 1000)))
          (let [result (.poll results 100 TimeUnit/MILLISECONDS)]
            (if (nil? result)
              (recur inp last-refresh)
              (let [rr (:rr result)
                    job (:job result)
                    failed? (or
                              (:error result)
                              (not (= (:result result) :success)))
                    nxt (cond
                          (not failed?) inp
                          (< (:ttl job) 1) inp
                          :else (cons (assoc job :ttl (dec (:ttl job))) inp))]
                (when-not (instance? java.net.ConnectException (:error result))
                  (.addFirst empties rr))
                (recur nxt last-refresh))))
        :else
          (let [[job & rst] inp
                rr (.removeLast empties)
                job (if (:ttl job) job (assoc job :ttl 20))
                result-map {:rr rr :job job}]
            (d/on-realized
              ((:thunk rr) rr job)
              (fn [result]
                (.put results
                  (assoc result-map :result result)))
              (fn [error]
                (.put results
                  (assoc result-map :error error))))
            (recur rst last-refresh))))))
