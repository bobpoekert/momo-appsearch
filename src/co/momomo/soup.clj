(ns co.momomo.soup
  (import [org.jsoup Jsoup]
          [org.jsoup.nodes Node Element]
          [org.jsoup.select Collector Evaluator CombiningEvaluator CombiningEvaluator$And CombiningEvaluator$Or
           StructuralEvaluator CombiningEvaluatorWrapper]
          [co.momomo EvaluatorFn]))

(set! *warn-on-reflection* true)

(defn ^String get-text
  [^Element e]
  (.text e))

(defn select
  [^Element root ^Evaluator selector]
  (Collector/collect selector root))

(defn select-strip
  [tree selection]
  (if-let [^Element v (first (select tree selection))]
    (.trim (.text v))))

(defn getattr
  [^Element e ^String k]
  (if e
    (.get (.attributes e) k)))

(defn select-attr
  [tree attr selection]
  (->>
    (select tree selection)
    (map #(getattr % attr))))

(defmacro evaluator
  [nom args class-name & constructor-args]
  `(defn ~nom ~args
    (~(symbol (str "org.jsoup.select.Evaluator$" (name class-name) ".")) 
     ~@constructor-args)))

(defmacro sv
  [nom & args]
  (let [sym (symbol (str "co.momomo.StructuralEvaluator$" (name nom) "."))]
    (cons sym args)))

(defn path
  [& selectors]
  (reduce
    (fn [^Evaluator res ^Evaluator v]
      (CombiningEvaluatorWrapper/makeAnd [v (sv ImmediateParent res)]))
    selectors))

(defn any-path
  [& selectors]
  (reduce
    (fn [^Evaluator res ^Evaluator v]
      (CombiningEvaluatorWrapper/makeAnd [v (sv Parent res)]))
    selectors))

(defn any-pos
  [^Evaluator e]
  (CombiningEvaluatorWrapper/makeOr [e (sv Parent e)]))

(defn ^Evaluator ev
  [thunk]
  (EvaluatorFn. thunk)) 

(defn %and
  [& selectors]
  (CombiningEvaluatorWrapper/makeAnd selectors))

(defn %or
  [& args]
  (CombiningEvaluatorWrapper/makeOr args))

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
  (sv ImmediateParent e))

(defn has-class
  [class-name]
  (ev
    (fn [^Element root ^Element e]
      (contains? (.classNames e) class-name))))

(evaluator has-attr
  [^String k]
  Attribute k)

(evaluator wildcard
  [] AllElements)

(evaluator kv
  [^String k ^String v]
  AttributeWithValue k v)

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

(evaluator -nth-of-type
  [a b]
  IsNthOfType a b)

(defn nth-of-type
  ([a b] (-nth-of-type a b))
  ([a] (-nth-of-type 1 a)))
