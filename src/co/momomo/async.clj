(ns co.momomo.async
  (require [clojure.core.async :refer [go]]))

(defmacro gocatch
  [& bodies]
  `(go
    (try
      ~@bodies
      (catch Throwable e# e#))))
