(ns co.momomo.appsearch.auth
  (require [clojure.java.jdbc :as db]))

(def db-config {
  :classname "org.sqlite.JDBC"
  :subprotocol "sqlite"
  :subname "auth.sqlite"})

(defn create-db!
  []
  (db/db-do-commands db-config
    (db/create-table-ddl :play_accounts
      [[:id :integer :primary :key] 
       [:email :text]
       [:password :text]
       [:token :text]
       [:device_properties :text]
       [:created_on :long]
       [:token_created_on :long]
       [:last_login :long]])))

(def maybe-create-db!
  (delay
    (if-not (.exists (java.io.File. "auth.sqlite"))
      (create-db!))))

(defn add-account!
  [email password device-properties]
  @maybe-create-db!
  (db/insert! db-config :play_accounts {
    :email email
    :password password
    :token nil
    :device_properties device-properties
    :created_on (System/currentTimeMillis)
    :token_created_on nil
    :last_login nil}))

(defn set-account-token!
  [id token]
  @maybe-create-db!
  (db/update! db-config :play_accounts
    {:token token
     :token_created_on (System/currentTimeMillis)
     :last_login (System/currentTimeMillis)}
    ["id = ?" id]))
   
(defn get-accounts
  []
  @maybe-create-db!
  (db/query db-config "select * from play_accounts"))
