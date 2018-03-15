(ns co.momomo.crawler
  (require [co.momomo.cereal :as cereal]
           [clj-http.client :as client]
           [clj-http.core :as hc]
           [clj-http.cookies :as cookies]
           [clojure.data.xml :as xml]
           [clojure.java.io :as io]
           [clojure.string :as ss])
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
      (client/get "http://filefab.com/api.php?l=4YlFRFXacsdxd7X3Tt8nz-idps0TbEXOUZd-CpX1XuE")
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
  http-opts cookies score])

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

(defn build-requester-state
  [requester-fns]
  (let [res (PriorityBlockingQueue.
              (* 2 (count requester-fns) (count @proxies))
              (comparator (fn [a b] (if (< (:score a) (:score b)) -1 1))))]
    (doseq [thunk requester-fns
            [proxy-host proxy-port] @proxies]
      (.add res
        (->Requester thunk 0 0
          {:headers {"User-Agent" (pick-random @user-agents)}
           :proxy-host proxy-host :proxy-port proxy-port
           :conn-timeout conn-timeout :socket-timeout 5000
           :retry-handler (fn [ex try-cnt ctx] false)}
          (cookies/cookie-store) 0.001)))
    (doseq [thunk requester-fns]
      (.add res
        (->Requester thunk 0 0
          {:headers {"User-Agent" (pick-random @user-agents)}
           :conn-timeout conn-timeout
           :retry-handler (fn [ex try-cnt ctx] false)}
          (cookies/cookie-store) 0.001)))
    res))
     
(defn run-request
  [^PriorityBlockingQueue requesters resource]
  ((fn looper [requester retries]
    (prn [(:success-count requester) (:failure-count requester)])
    (binding [*http-opts* (:http-opts requester)
              hc/*cookie-store* (:cookies requester)]
      (let [res (try
                  ((:thunk requester) resource)
                  (catch Exception e (prn (.getMessage e)) ::fail))]
        (if (= res ::fail)
          (do
            (.add requesters
              (-> requester
                (assoc :failure-count (inc (:failure-count requester)))
                (update-requester-score)))
            (if (< retries 100)
              (looper (.take requesters) (inc retries))
              false))
          (do
            (.add requesters
              (-> requester
                (assoc :success-count (inc (:success-count requester)))
                (update-requester-score)))
            res))))) (.take requesters) 0))


(defn crawl
  [inp opts requester-fns]
  (let [requester-state (build-requester-state requester-fns)]
    (cereal/parmap inp opts (map (partial run-request requester-state)))))
