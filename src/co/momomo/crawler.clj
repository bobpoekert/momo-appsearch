(ns co.momomo.crawler
  (require [co.momomo.cereal :as cereal]
           [clojure.data.xml :as xml]
           [clojure.java.io :as io]
           [clojure.string :as ss]
           [slingshot.slingshot :refer [try+ throw+]]
           [clojure.core.async :as async]
           [clj-http.client :as client]
           [co.momomo.async :refer [gocatch]]
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

(def proxies
  (delay
    (concat
      (for [[host port] (proxy-hosts "http://filefab.com/api.php?l=Od9bhDRsZz5XCq2_waX09Wcll-DtXOXvHJNe68cZblE")]
        [:http host port])
      (for [[host port] (proxy-hosts "http://filefab.com/api.php?l=jcW8aUaS9GCCqiDQDS9tu7RU8CCsNVthZuWPAU2dzcI")]
        [:socks4 host port])
      (for [[host port] (proxy-hosts "http://filefab.com/api.php?l=P9TdKt1_-xtqwtlCXVASfpGQ6-i3cxYgvTD8Bs2HUSU")]
        [:socks5 host port]))))

(defn pick-random
  [v]
  (nth v (int (* (Math/random) (count v)))))

(defrecord Requester [thunk client headers])

(def req http/req)

(defn get-body-or-throw
  [res]
  (prn (:status res))
  (if (= (:status res) 200)
    (:body res)
    (throw (RuntimeException. (str "status: " (:status res))))))

(defn make-requester
  [thunk proxy-protocol proxy-host proxy-port]
  (->Requester thunk
    (http/build-client proxy-protocol proxy-host proxy-port) 
    {"User-Agnent" (pick-random @user-agents)}))

(defn requesters
  [requester-fns]
  (concat
    (for [thunk requester-fns [proxy-type proxy-host proxy-port] @proxies]
      (make-requester thunk proxy-type proxy-host proxy-port))
    (for [thunk requester-fns]
      (make-requester thunk nil nil nil))))

(defn crawl
  [inp opts requester-fns]
  (let [^ArrayDeque empties (ArrayDeque. ^java.util.List (take 10 (requesters requester-fns)))
        ^HashMap chan-rrs (HashMap.)
        ^HashMap chan-jobs (HashMap.)]
    (loop [inp inp]
      (prn (:artifact_name (first inp)))
      (if (empty? empties)
        (let [_ (prn "alts waiting")
              [v chan] (async/alts!! (vec (keys chan-rrs)))
              _ (prn "alts got")
              rr (get chan-rrs chan)
              job (get chan-jobs chan)
              nxt (cond
                    (= v :success) inp
                    (< (:ttl job) 1) inp
                    :else (cons (assoc job :ttl (dec (:ttl job))) inp))]
          (.add empties rr)
          (.remove chan-rrs chan)
          (.remove chan-jobs job)
          (recur nxt))
        (let [[job & rst] inp
              job (assoc job :ttl 100)
              rr (.remove empties)
              chan ((:thunk rr) rr job)]
          (.put chan-rrs chan rr)
          (.put chan-jobs chan job)
          (recur rst))))))
