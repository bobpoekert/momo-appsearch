(ns co.momomo.http
  (require [manifold.deferred :as d]
           [clojure.data.xml :as xml]
           [clojure.java.io :as io])
  (import [org.asynchttpclient Dsl AsyncHttpClient RequestBuilder
            ListenableFuture Response AsyncCompletionHandler HttpResponseBodyPart
            AsyncHandler$State]
          [org.asynchttpclient.proxy ProxyType]
          [java.util.concurrent Executors Executor TimeUnit ThreadFactory]
          [java.util.concurrent.atomic AtomicReference]
          [io.netty.handler.codec.http DefaultHttpHeaders HttpHeaders]
          [io.netty.handler.codec.http.cookie Cookie]
          [io.netty.util HashedWheelTimer TimerTask]
          [io.netty.channel.epoll Epoll EpollEventLoopGroup]
          [io.netty.channel.nio NioEventLoopGroup]))

(set! *warn-on-reflection* true)

(def async-executor
  (delay
    (Executors/newFixedThreadPool
      (max 1 (dec (.availableProcessors (Runtime/getRuntime))))
      (Executors/defaultThreadFactory))))

(def netty-timer
  (delay (HashedWheelTimer.)))

(def event-loop-group
  (delay
    (if (Epoll/isAvailable)
      (EpollEventLoopGroup. (max 1 (dec (.availableProcessors (Runtime/getRuntime)))))
      (NioEventLoopGroup. 8))))

(defn after-time
  [millis thunk]
  (.newTimeout
    ^HashedWheelTimer @netty-timer
    (proxy [TimerTask] []
      (run [t]
        (thunk)))
    (long millis) TimeUnit/MILLISECONDS))

(defn time-deferred
  [millis]
  (let [res (d/deferred)]
    (after-time millis
      #(d/success! res true))
    res))

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


(defn build-client
  ([proxy-type proxy-host proxy-port connect-timeout]
    (let [config (->
                  (Dsl/config)
                  (.setNettyTimer @netty-timer)
                  (.setEventLoopGroup @event-loop-group)
                  (.setMaxRequestRetry 0)
                  (.setFollowRedirect true)
                  (.setRequestTimeout (* 20 60 1000))
                  (.setConnectTimeout connect-timeout))]
      (if (nil? proxy-type)
        (Dsl/asyncHttpClient config)
        (Dsl/asyncHttpClient
          (->
            config
            (.setProxyServer 
              (->
                (Dsl/proxyServer proxy-host proxy-port)
                (.setProxyType
                  (case proxy-type
                    :http ProxyType/HTTP
                    :socks4 ProxyType/SOCKS_V4
                    :socks5 ProxyType/SOCKS_V5))
                (.build)))
            (.build))))))
  ([proxy-type proxy-host proxy-port]
    (build-client proxy-type proxy-host proxy-port 100)))

(defn pick-random
  [v]
  (nth v (int (* (Math/random) (count v)))))

(defn random-user-agent
  []
  (pick-random @user-agents))

(defn make-requester
  [& args]
  {:client (apply build-client args)
   :headers {"User-Agent" (random-user-agent)}})

(def default-requester (delay (make-requester nil nil nil 1000)))

(defn process-response
  [^Response response opts]
  {:status (.getStatusCode response)
   :status-text (.getStatusText response)
   :body (cond
           (:body-callback opts) nil
           (= (:as opts) :byte-array) (.getResponseBodyAsBytes response)
           (= (:as opts) :byte-buffer) (.getResponseBodyAsByteBuffer response)
           (= (:as opts) :stream) (.getResponseBodyAsStream response)
           (string? (:as opts)) (.getResponseBody response ^String (:as opts))
           :else (.getResponseBody response))
    :headers (into {}
              (map (fn [v] [(.toLowerCase ^String (key v)) (val v)])
                (.getHeaders response)))
    :cookies (.getCookies response)})

(defn req
  ([^String url requester thunk opts]
    (let [^AsyncHttpClient client (:client requester)
          res (d/onto (d/deferred) @async-executor)
          ^RequestBuilder req (case thunk
                                :get (.prepareGet client url)
                                :connect (.prepareConnect client url)
                                :options (.prepareOptions client url)
                                :put (.preparePut client url)
                                :patch (.preparePatch client url)
                                :trace (.prepareTrace client url)
                                :post (.preparePost client url)
                                :delete (.prepareDelete client url)
                                :head (.prepareHead client url))]
      (doseq [[k v] (:headers requester)]
        (.setHeader req (str k) v))
      (when-not (nil? (:headers opts))
        (doseq [[k v] (:headers opts)]
          (.setHeader req (str k) v)))
      (when-not (nil? (:form opts))
        (->>
          (:form opts)
          (map (fn [[k v]] [(str k) (if (string? v) (list v) (map str v))]))
          ^java.util.Map (into {})
          (.setFormParams req)))
      (when-not (nil? (:body opts))
        (.setBody req (:body opts)))
      (let [cb (:body-callback opts)]
        (.executeRequest client (.build req)
          (proxy [AsyncCompletionHandler] []
            (onCompleted [^Response response]
              (let [rm (process-response response opts)
                    error! #(d/error! res
                            (ex-info
                              (str "http error: " (:status rm))
                              rm))]
                (cond
                  (or (false? (:throw-exceptions opts)) (= (:status rm) 200)) (d/success! res rm)
                  :else (error!))))
            (onThrowable [^Throwable v]
              (d/error! res v))
            (onBodyPartReceived [^HttpResponseBodyPart part]
              (if (nil? cb)
                (proxy-super onBodyPartReceived part)
                (do
                  (cb (.getBodyPartBytes part))
                  AsyncHandler$State/CONTINUE))))))
      res))
  ([url requester thunk]
    (req url requester thunk {}))
  ([url thunk]
    (req url @default-requester thunk)))

(defn- inner-req-retry
  [url requesters args error]
  (if (seq requesters)
    (d/catch
      (apply req url (first requesters) args)
      (partial inner-req-retry url (rest requesters) args))
    (d/error-deferred error)))

(defn req-retry
  [url requesters & args]
  (inner-req-retry url requesters args nil))
