(ns co.momomo.appsearch.googleplay
  (require [clojure.java.io :as io]
           [co.momomo.appsearch.auth :as auth])
  (import [java.util Properties Locale] 
          [java.util.concurrent BlockingQueue LinkedBlockingQueue]
          [com.github.yeriomin.playstoreapi
            GooglePlayAPI PlayStoreApiBuilder PropertiesDeviceInfoProvider]
          [co.momomo ApacheHttpClientAdapter ObjectPool]))

(defmacro gen-device-properties-file-names
  []
  (->>
    (file-seq (java.io.File. "./resources"))
    (map #(.getName %))
    (filter #(.endsWith % ".properties"))
    (into [])))

(def device-properties-file-names (gen-device-properties-file-names))

(defn load-resource-properties
  [resource-name]
  (with-open [inf (io/input-stream (io/resource resource-name))]
    (let [res (Properties.)]
      (.load res inf)
      res)))

(def device-properties
  (delay
    (into {}
      (for [f device-properties-file-names]
        [f (load-resource-properties f)]))))

(defn pick-random
  [v]
  (nth v (rand-int (count v))))

(defn create-new-account!
  [email password]
  (let [device (pick-random device-properties-file-names)]
    (auth/add-account! email password device)))

(defn make-device-info-provider
  [properties-name]
  (let [res (PropertiesDeviceInfoProvider.)]
    (.setProperties res (get @device-properties properties-name))
    (.setLocaleString res (.toString Locale/ENGLISH))
    res))

(defn create-play-context
  [acct]
  (let [builder
            (->
              (PlayStoreApiBuilder.)
              (.setEmail (:email acct))
              (.setPassword (:password acct))
              (.setHttpClient (ApacheHttpClientAdapter.))
              (.setDeviceInfoProvider (make-device-info-provider (:device_properties acct))))]
    (if (:token acct)
      (->
        builder
        (.setToken (:token acct))
        (.build))
      (let [ctx (.build builder)]
        (auth/set-account-token! (:id acct) (.getToken ctx))
        ctx))))

(def ^ObjectPool play-contexts
  (ObjectPool. (map create-play-context (auth/get-accounts))))

(defn get-context!
  []
  (.take play-contexts))

(defn restore-context!
  [ctx]
  (.put play-contexts ctx))

(def ^:dynamic thread-context nil)

(defmacro with-context
  [ctx-name & bodies]
  (let [ctx-name (with-meta ctx-name {:tag GooglePlayAPI})]
   `(if (nil? thread-context)
      (binding [thread-context (get-context!)]
        (let [~ctx-name thread-context]
          (try
            ~@bodies
            (finally
              (restore-context! ~ctx-name)))))
      (let [~ctx-name thread-context]
        ~@bodies))))

(defn get-app-details
  [^String package-name]
  (with-context ctx
    (.details ctx package-name)))

(defn get-delivery-data
  ([^String package-name version-code offer-type]
    (with-context ctx
      (let [buy-response (.purchase ctx package-name ^int version-code ^int offer-type)]
        buy-response)))
  ([^String package-name]
    (let [details (get-app-details package-name)
          offers (-> details
                  (.getDocV2)
                  (.getOfferList))
          app-details (-> details
                        (.getDocV2)
                        (.getDetails)
                        (.getAppDetails))
          version-code (.getVersionCode app-details)]
      (for [offer offers]
        (get-delivery-data package-name
          version-code
          (.getOfferType offer))))))

(defn get-signature
  [package-name]
  (for [offer (get-delivery-data package-name)]
    (.getSignature offer)))

;; (get-signature "com.mobulasoft.criticker")
