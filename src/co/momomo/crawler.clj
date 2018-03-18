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
          [java.net Socket InetSocketAddress]))

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
      (.connect (InetSocketAddress. host port) 20))
    true
    (catch Exception e false)))

(def proxies
  (delay
    (->>
      (client/get "http://filefab.com/api.php?l=eMiLb9dfodyyKdqsj8d4cAtZNgth_CSJhTV5oDsCbdk")
      (:body)
      (ss/split-lines)
      (filter (fn [^String v] (> (.indexOf v ":") -1)))
      (map (fn [^String v]
            (let [v (ss/replace v #"\</?pre\>" "")
                  [ip port] (ss/split v #":")]
              [ip (Integer/parseInt port)])))
      (vec))))

(defn pick-random
  [v]
  (nth v (int (* (Math/random) (count v)))))

(defrecord Requester [thunk client headers cookies])

(def req http/req)

(defn get-body-or-throw
  [res]
  (if (= (:status res) 200)
    (:body res)
    (throw (RuntimeException.))))

(defn make-requester
  [thunk proxy-protocol proxy-host proxy-port]
  (->Requester thunk
    (http/build-client proxy-protocol proxy-host proxy-port) 
    {"User-Agnent" (pick-random @user-agents)}
    (atom [])))

(defn requesters
  [requester-fns]
  (let [retry (fn [ex try-cnt ctx] false)]
    (concat
      (for [thunk requester-fns [proxy-host proxy-port] @proxies]
        (make-requester thunk :http proxy-host proxy-port))
      (for [thunk requester-fns]
        (make-requester thunk nil nil nil)))))

(defn crawl
  [inp opts requester-fns]
  (let [inchan (async/chan 50)
        rrs (vec (requesters requester-fns))
        crawler-chan
          (gocatch
            (loop [requester-jobs {}
                   requester-chans {}
                   requester-empties rrs]
              (if (empty? requester-empties)
                (let [[v port] (async/alts! (vec (keys requester-chans)))
                      rr (get requester-chans port)]
                  (when-not (= v :success)
                    (when (instance? Throwable v)
                      (prn (.getMessage ^Throwable v)))
                    (async/put! inchan (get requester-jobs port)))
                  (recur
                    (dissoc requester-jobs port)
                    (dissoc requester-chans port)
                    (cons rr requester-empties)))
                (let [[rr & rrs] requester-empties
                      job (async/<! inchan)
                      rchan ((:thunk rr) rr job)]
                  (recur
                    (assoc requester-jobs rchan job)
                    (assoc requester-chans rchan rr)
                    rrs)))))]
    (doseq [row inp]
      (prn (:artifact_name row))
      (async/>!! inchan row))
    (async/<!! crawler-chan)))
