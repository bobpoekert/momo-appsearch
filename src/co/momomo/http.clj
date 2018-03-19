(ns co.momomo.http
  (require [clojure.core.async :as async]
           [clojure.core.async.impl.concurrent :as conc])
  (import [org.asynchttpclient Dsl AsyncHttpClient RequestBuilder]
          [org.asynchttpclient ListenableFuture Response]
          [org.asynchttpclient.proxy ProxyType]
          [java.util.concurrent Executors Executor TimeUnit]
          [io.netty.handler.codec.http DefaultHttpHeaders HttpHeaders]
          [io.netty.handler.codec.http.cookie Cookie]
          [io.netty.util HashedWheelTimer TimerTask]
          [io.netty.channel.epoll Epoll EpollEventLoopGroup]
          [io.netty.channel.nio NioEventLoopGroup]))

(set! *warn-on-reflection* true)

(def async-executor
  (delay
    (Executors/newFixedThreadPool 1
      (conc/counted-thread-factory "netty core async bridge" true))))

(def netty-timer
  (delay (HashedWheelTimer.)))

(def event-loop-group
  (delay
    (if (Epoll/isAvailable)
      (EpollEventLoopGroup. 1)
      (NioEventLoopGroup. 8))))

(defn after-time
  [millis thunk]
  (.newTimeout
    ^HashedWheelTimer @netty-timer
    (proxy [TimerTask] []
      (run [t]
        (thunk)))
    millis TimeUnit/MILLISECONDS))

(defn build-client
  [proxy-type proxy-host proxy-port]
  (let [config (->
                (Dsl/config)
                (.setNettyTimer @netty-timer)
                (.setEventLoopGroup @event-loop-group)
                (.setMaxRequestRetry 0)
                (.setFollowRedirect true)
                (.setConnectTimeout 100)
                (.setRequestTimeout 10000))]
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

(defn process-response
  [^Response response response-as]
  {:status (.getStatusCode response)
   :status-text (.getStatusText response)
   :body (case response-as
           :byte-array (.getResponseBodyAsBytes response)
           :byte-buffer (.getResponseBodyAsByteBuffer response)
           :stream (.getResponseBodyAsStream response)
           (if (string? response-as)
            (.getResponseBody response ^String response-as)
            (.getResponseBody response)))
    :headers (into {}
              (map (fn [v] [(.toLowerCase ^String (key v)) (val v)])
                (.getHeaders response)))
    :cookies (.getCookies response)})

(defn req
  ([^String url requester thunk opts]
    (let [^AsyncHttpClient client (:client requester)
          res (async/chan)
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
      (when-not (nil? (:body opts))
        (.setBody req (:body opts)))
      (let [^ListenableFuture f (.executeRequest client (.build req))]
        (.addListener f 
          (fn []
            (let [^Response response (.get f)
                  rm (process-response response (:as opts))]
              (prn (:status rm) (:status-text rm))
              ;(if (= (:status rm) 503)
              ;  (after-time 1000 #(async/>!! res rm))
                (async/>!! res rm)))
          ^Executor @async-executor))
      res))
  ([url requester thunk]
    (req url requester thunk {}))
  ([url requester]
    (req url requester :get)))
