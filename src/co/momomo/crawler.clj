(ns co.momomo.crawler
  (require [co.momomo.cereal :as cereal]
           [clojure.data.xml :as xml]
           [clojure.java.io :as io]
           [clojure.string :as ss]
           [slingshot.slingshot :refer [try+ throw+]]
           [clj-http.client :as client]
           [manifold.deferred :as d]
           [co.momomo.http :as http])
  (import [java.util.concurrent PriorityBlockingQueue LinkedBlockingQueue]
          [java.net Socket InetSocketAddress]
          [java.util.concurrent.locks ReentrantLock]
          [java.util ArrayDeque HashMap]))

(set! *warn-on-reflection* true)

(def user-agents
  (delay
    (->>
      (io/resource "useragentswitcher.xml")
      (io/reader)
      (xml/parse)
      (tree-seq #(not (nil? (:content %))) :content)
      (map #(:useragent (:attrs %)))
      (filter (complement nil?))
      (vec))))

(defn host-alive?
  [[host port]]
  (try
    (doto (Socket.)
      (.connect (InetSocketAddress. ^String host ^Integer port) 20))
    true
    (catch Exception e false)))


(defn proxy-hosts
  [list-url]
  (->>
    (client/get list-url)
    (:body)
    (ss/split-lines)
    (filter (fn [^String v] (> (.indexOf v ":") -1)))
    (map (fn [^String v]
          (let [v (ss/replace v #"\</?pre\>" "")
                [ip port] (ss/split v #":")]
            [ip (Integer/parseInt port)])))
    (vec)))

(defn get-proxies
  []
  (into #{}
    (concat
      (for [[host port] (proxy-hosts "http://filefab.com/api.php?l=Od9bhDRsZz5XCq2_waX09Wcll-DtXOXvHJNe68cZblE")]
        [:http host port])
      (for [[host port] (proxy-hosts "http://filefab.com/api.php?l=jcW8aUaS9GCCqiDQDS9tu7RU8CCsNVthZuWPAU2dzcI")]
        [:socks4 host port])
      (for [[host port] (proxy-hosts "http://filefab.com/api.php?l=P9TdKt1_-xtqwtlCXVASfpGQ6-i3cxYgvTD8Bs2HUSU")]
        [:socks5 host port]))))

(def proxies (delay (get-proxies)))

(defn pick-random
  [v]
  (nth v (int (* (Math/random) (count v)))))

(defrecord Requester [thunk client headers])

(def req http/req)

(defn make-requester
  [thunk proxy-protocol proxy-host proxy-port]
  (->Requester thunk
    (http/build-client proxy-protocol proxy-host proxy-port) 
    {"User-Agent" (pick-random @user-agents)}))

(defn requesters
  [requester-fns]
  (for [thunk requester-fns [proxy-type proxy-host proxy-port] (get-proxies)]
    (make-requester thunk proxy-type proxy-host proxy-port)))

(defn crawl
  [inp opts requester-fns]
  (let [get-empties #(ArrayDeque. ^java.util.Collection (requesters requester-fns))
        ^LinkedBlockingQueue results (LinkedBlockingQueue. 20)]
    (loop [inp inp
           last-refresh (System/currentTimeMillis)
           ^ArrayDeque empties (get-empties)] 
      (cond
        (>= (- (System/currentTimeMillis) last-refresh) (* 30 60 1000))
          (recur inp (System/currentTimeMillis) (get-empties))
        (< (.size empties) 1) 
          (let [result (.take results)
                rr (:rr result)
                job (:job result)
                failed? (or
                          (:error result)
                          (not (= (:result result) :success)))
                nxt (cond
                      (not failed?) inp
                      (< (:ttl job) 1) inp
                      :else (cons (assoc job :ttl (dec (:ttl job))) inp))]
            (.addFirst empties rr)
            (recur nxt last-refresh empties))
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
            (recur rst last-refresh empties))))))
