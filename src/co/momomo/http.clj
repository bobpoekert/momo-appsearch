(ns co.momomo.http
  (require [clojure.core.async :as async])
  (import [org.asynchttpclient Dsl AsyncHttpClient RequestBuilder]
          [org.asynchttpclient ListenableFuture Response]
          [org.asynchttpclient.proxy ProxyType]
          [io.netty.handler.codec.http DefaultHttpHeaders HttpHeaders]
          [io.netty.handler.codec.http.cookie Cookie]))

(defn build-client
  [proxy-type proxy-host proxy-port]
  (if (nil? proxy-type)
    (Dsl/asyncHttpClient (Dsl/config))
    (Dsl/asyncHttpClient
      (->
        (Dsl/config)
        (.setProxyServer 
          (->
            (Dsl/proxyServer proxy-host proxy-port)
            (.setProxyType
              (case proxy-type
                :http ProxyType/HTTP
                :socks4 ProxyType/SOCKS_V4
                :socks5 ProxyType/SOCKS_V5))
            (.build)))
        (.build)))))

(defn process-response
  [^Response response response-as]
  {:status (.getStatusCode response)
   :status-text (.getStatusText response)
   :body (case response-as
           :byte-array (.getResponseBodyAsBytes response)
           :byte-buffer (.getResponseBodyAsByteBuffer response)
           :stream (.getResponseBodyAsInputStream response)
           (if (string? response-as)
            (.getResponseBody response ^String response-as)
            (.getResponseBody response)))
    :headers (into {} (.getHeaders response))
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
      (.setCookies req @(:cookies requester))
      (let [^ListenableFuture f (.execute req)]
        (.addListener f 
          (fn []
            (let [^Response response (.get f)]
              (swap! (:cookies requester) #(into % (.getCookies response)))
              (async/>!! res (process-response response (:as opts)))))
          clojure.core.async.impl.dispatch/executor))
      res))
  ([url requester thunk]
    (req url requester thunk {}))
  ([url requester]
    (req url requester :get)))
