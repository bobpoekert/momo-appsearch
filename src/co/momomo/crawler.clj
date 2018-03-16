(ns co.momomo.crawler
  (require [co.momomo.cereal :as cereal]
           [clj-http.client :as client]
           [clj-http.core :as hc]
           [clj-http.cookies :as cookies]
           [clojure.data.xml :as xml]
           [clojure.java.io :as io]
           [clojure.string :as ss]
           [clojure.pprint :refer [pprint]])
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
  ([url thunk opts & args]
    (let [opts (merge *http-opts* opts)]
      (apply (get http-thunks thunk) url opts args)))
  ([url thunk]
    (req url thunk {}))
  ([url]
    (req url :get)))

(defrecord Requester [
  thunk success-count failure-count
  http-opts cookies score last-update])

(defn requester-score
  [requester]
  (*
    (* (Math/random) 0.5) ; epsilon
    (+ 0.0001 ; anti-vanising constant
      (/ (:success-count requester)
         (+ (:success-count requester) (:failure-count requester))))))

(defn update-requester-score
  [requester]
  (assoc requester :score (requester-score requester)))

(def conn-timeout 500)

(def ^ThreadLocal fail-count (ThreadLocal.))

(defn get-fail-count
  []
  (let [res (.get fail-count)]
    (if (nil? res) 0 res)))

(defn reset-fail-count!
  []
  (.set fail-count 0))

(defn inc-fail-count!
  []
  (.set fail-count (inc (get-fail-count))))

(def ^:dynamic *http-error-handler* nil)

(def error-stats (atom {}))

(defn crawl-thread
  [^LinkedBlockingQueue inq requester]
  (cereal/thread "crawler"
    (fn []
      (while true 
        (try
          (binding [*http-opts* (:http-opts requester)
                    hc/*cookie-store* (:cookies requester)]
            (let [resource (.take inq)
                  res (try
                        ((:thunk requester) resource)
                        (catch Exception e e))]
              (if (instance? Throwable res)
                (do
                  (.put inq resource)
                  (inc-fail-count!)
                  (swap! error-stats
                    (fn [v]
                      (let [m (.getMessage ^Exception res)
                            old (get v m)]
                        (assoc v m (if (not (nil? old)) (inc old) 1))))))
                (do
                  (reset-fail-count!)
                  (swap! error-stats
                    (fn [v] (assoc v :success (if (nil? (:success v)) 1 (inc (:success v))))))))
              (swap! (:last-update requester) (fn [v] (System/currentTimeMillis)))))
          (catch Exception e (prn e)))))))

(defn requesters
  [inq requester-fns]
  (let [res (java.util.ArrayList.)
        onerror (fn [ex try-cnt ctx] false)]
    (doseq [thunk requester-fns 
           [proxy-host proxy-port] @proxies]
      (.add res
        (crawl-thread inq
          (->Requester thunk 0 0
              {:headers {"User-Agent" (pick-random @user-agents)}
               :proxy-host proxy-host :proxy-port proxy-port
               :conn-timeout conn-timeout :socket-timeout 5000
               :retry-handler onerror}
              (cookies/cookie-store) 0.001 (atom (System/currentTimeMillis))))))
      res))

(defn crawl
  [inp opts requester-fns]
  (let [^LinkedBlockingQueue inq (LinkedBlockingQueue. 100)]
    (requesters inq requester-fns)
    (cereal/thread "crawl printer"
      (doseq [row inp]
        (.put inq row)))
    (while true
      (Thread/sleep 500)
      (pprint @error-stats))))
