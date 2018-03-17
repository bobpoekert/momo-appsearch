(ns co.momomo.crawler
  (require [co.momomo.cereal :as cereal]
           [clj-http.client :as client]
           [clj-http.core :as hc]
           [clj-http.cookies :as cookies]
           [clojure.data.xml :as xml]
           [clojure.java.io :as io]
           [clojure.string :as ss]
           [slingshot.slingshot :refer [try+ throw+]])
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
      (try+
        (apply (get http-thunks thunk) url opts args)
        (catch [:status 503] _
          (Thread/sleep 200)
          (throw+)))))
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
            (cookies/cookie-store) 0.001 (atom (System/currentTimeMillis))))
      (for [thunk requester-fns]
        (->Requester thunk 0 0
            {:headers {"User-Agent" (pick-random @user-agents)}
             :conn-timeout conn-timeout
             :retry-handler retry}
            (cookies/cookie-store) 0.001 (atom (System/currentTimeMillis)))))))

(defn crawl-thread
  [^LinkedBlockingQueue inq requester]
  (cereal/thread "crawler"
    (fn []
      (while true
        (try
          (let [resource (.take inq)]
            (binding [*http-opts* (:http-opts requester)
                      hc/*cookie-store* (:cookies requester)]
              (let [res (try
                          ((:thunk requester) resource)
                          (catch Exception e (prn (.getMessage e)) ::fail))]
                (when (= res :fail)
                  (.put inq resource))
                (swap! (:last-update requester) (fn [v] (System/currentTimeMillis))))))
          (catch InterruptedException e nil))))))

(defn crawl
  [inp opts requester-fns]
  (let [^LinkedBlockingQueue inq (LinkedBlockingQueue. 100)
        reqs (vec (requesters requester-fns))
        threads (vec (map (partial crawl-thread inq) (requesters requester-fns)))]
    (when-not (nil? (:timeout opts))
      (cereal/thread "crawler interrupter"
        (fn []
          (while true
            (Thread/sleep 100)
            (loop [reqs reqs threads threads]
              (when (and reqs threads)
                (let [req (first reqs) thread (first threads)]
                  (when (> (- (System/currentTimeMillis)
                              @(:last-update req))
                           (:timeout opts))
                    (.interrupt ^Thread thread)))
                (recur (rest reqs) (rest threads))))))))
    (doseq [row inp]
      (.put inq row))))
