(ns co.momomo.crawler
  (require [co.momomo.cereal :as cereal]
           [clj-http.client :as client]
           [clj-http.core :as hc]
           [clj-http.cookies :as cookies]
           [clojure.data.xml :as xml]
           [clojure.java.io :as io]
           [clojure.string :as ss]
           [slingshot.slingshot :refer [try+ throw+]]
           [clojure.core.async :as async]
           [co.momomo.async :refer [gocatch]])
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

(def ^:dynamic *http-opts* {})

(def http-thunks
  {:get client/get
   :post client/post
   :head client/head
   :put client/put})

(defn req
  ([url requester thunk opts]
    (let [res (async/chan)
          opts (merge (:http-opts requester) opts)
          opts (assoc opts :async? true)
          responder (fn [response] 
                      (async/>!! res response))]
      (binding [hc/*cookie-store* (:cookies requester)]
        ((get http-thunks thunk) url opts responder responder))
      res))
  ([url requester thunk]
    (req url requester thunk {}))
  ([url requester]
    (req url requester :get)))

(defrecord Requester [
  thunk success-count failure-count
  http-opts cookies last-update])

(def conn-timeout 500)

(defn requesters
  [requester-fns]
  (let [retry (fn [ex try-cnt ctx] false)]
    (concat
      (for [thunk requester-fns [proxy-host proxy-port] @proxies]
        (->Requester thunk 0 0
            {:headers {"User-Agent" (pick-random @user-agents)}
             :proxy-host proxy-host :proxy-port proxy-port
             :conn-timeout conn-timeout :socket-timeout 5000
             :retry-handler retry}
            (cookies/cookie-store) (atom (System/currentTimeMillis))))
      (for [thunk requester-fns]
        (->Requester thunk 0 0
            {:headers {"User-Agent" (pick-random @user-agents)}
             :conn-timeout conn-timeout
             :retry-handler retry}
            (cookies/cookie-store) (atom (System/currentTimeMillis)))))))

(defn crawl
  [inp opts requester-fns]
  (let [inq (LinkedBlockingQueue. 50) 
        inchan (async/chan 50)
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
                    (.put inq (get requester-jobs port)))
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
    (cereal/thread "chan putter"
      (fn []
        (while true
          (async/>!! inchan (.take inq)))))
    (doseq [row inp]
      (.put inq row))
    (async/<!! crawler-chan)))
