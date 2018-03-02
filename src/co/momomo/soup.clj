(ns co.momomo.soup
  (import [org.jsoup Jsoup]
          [org.jsoup.nodes Node Element]
          [org.jsoup.select Collector Evaluator CombiningEvaluator CombiningEvaluator$And CombiningEvaluator$Or
           StructuralEvaluator CombiningEvaluatorWrapper]
          [co.momomo EvaluatorFn]))

(defn select
  [^Element root ^Evaluator selector]
  (Collector/collect selector root))

(defmacro evaluator
  [nom args class-name & constructor-args]
  `(defn ~nom ~args
    (~(symbol (str "org.jsoup.select.Evaluator$" (name class-name) ".")) 
     ~@constructor-args)))

(defmacro sv
  [nom & args]
  (let [sym (symbol (str "org.jsoup.select.StructuralEvaluator$" (name nom) "."))]
    (cons sym args)))

(defn path
  [& selectors]
  (reduce
    (fn [^Evaluator res ^Evaluator v]
      (CombiningEvaluatorWrapper/makeAnd [v (sv Parent res)]))
    selectors))

(defn %and
  [& selectors]
  (CombiningEvaluatorWrapper/makeAnd selectors))

(defn %>
  [^Evaluator left ^Evaluator right]
  (CombiningEvaluatorWrapper/makeAnd [right (sv ImmediateParent left)]))

(defn %+
  [^Evaluator left ^Evaluator right]
  (CombiningEvaluatorWrapper/makeAnd [right (sv ImmediatePreviousSibling left)]))

(defn %<
  [^Evaluator left ^Evaluator right]
  (CombiningEvaluatorWrapper/makeAnd [right (sv PreviousSibling left)]))

(defn parent
  [^Evaluator e]
  (sv Parent e))

(defn %,
  [^Evaluator left ^Evaluator right]
  (if (instance? CombiningEvaluator$Or left)
    (do
      (.add ^CombiningEvaluator$Or left right)
      left)
    (let [res (CombiningEvaluatorWrapper/makeOr [])]
      (.add res right)
      (.add res left)
      res)))

(defn ^Evaluator ev
  [thunk]
  (EvaluatorFn. thunk)) 

(defn has-class
  [class-name]
  (ev
    (fn [^Element root ^Element e]
      (contains? (.classNames e) class-name))))

(defn desc-or-self
  [^Evaluator inner]
  (%, inner (sv ImmediateParent inner)))

(evaluator has-attr
  [^String k]
  Attribute k)

(evaluator wildcard
  [] AllElements)

(evaluator kv
  [^String k ^String v]
  AttributeKeyPair k v)

(evaluator attr-starts
  [^String k]
  AttributeStarting k)

(evaluator kv-val-contains
  [^String k ^String v]
  AttributeWithValueContaining k v)

(evaluator kv-val-endswith
  [^String k ^String v]
  AttributeWithValueEnding k v)

(evaluator kv-val-regex
  [^String k ^java.util.regex.Pattern v]
  AttributeWithValueMatching k v)

(evaluator kv-val-not
  [^String k ^String v]
  AttributeWithValueNot k v)

(evaluator kv-val-starting
  [^String k ^String v]
  AttributeWithValueStarting k v)

(evaluator cls
  [^String class-name]
  Class class-name)

(evaluator contains-data
  [^String data]
  ContainsData data)

(evaluator contains-own-text
  [^String text]
  ContainsOwnText text)

(evaluator tag
  [^String tag-name]
  Tag tag-name)

(evaluator id
  [^String id]
  Id id)
