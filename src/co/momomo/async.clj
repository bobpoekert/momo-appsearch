(ns co.momomo.async
  (require [manifold.deferred :as d]))

(defn inner->chain
  [forms x]
  (if (not forms)
    x
    (let [[h & t] forms]
      (cond
        (not (seq? h)) (inner->chain (list h x) t)
        (= (first h) 'd->)
          (let [vsym (gensym "d->arg")]
            `(d/chain ~x
              (fn ~(gensym "d-thread") [~vsym] ~(inner->chain t vsym))))
        :else (inner->chain t `(~(first h) ~x ~@(rest h)))))))

(defmacro ->chain
  [dv body]
  (inner->chain body dv))

(defn inner->>chain
  [forms x]
  (if (not forms)
    x
    (let [[h & t] forms]
      (cond
        (not (seq? h)) (inner->>chain (list h x) t)
        (= (first h) 'd->)
          (let [vsym (gensym "d->arg")]
            `(d/chain ~x
              (fn ~(gensym "d-thread") [~vsym] ~(inner->>chain t vsym))))
        :else (inner->>chain t `(~(first h) ~@(rest h) ~x))))))

(defmacro ->>chain
  [dv body]
  (inner->>chain body dv))
