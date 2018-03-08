(ns co.momomo.appsearch.apk
  (require [clojure.java.io :as io])
  (import [net.dongliu.apk.parser ApkFile AbstractApkFile ByteArrayApkFile]
          [net.dongliu.apk.parser.struct AndroidConstants]
          [com.android.apksig ApkVerifier ApkVerifier$Builder]
          [org.jf.dexlib2 Opcodes ValueType]
          [org.jf.dexlib2.dexbacked DexBackedDexFile
            DexBackedAnnotation DexBackedAnnotationElement DexBackedField
            DexBackedMethodImplementation DexBackedMethod DexBackedClassDef]
          [org.jf.dexlib2.iface.value EncodedValue]
          [org.jf.dexlib2.iface.debug DebugItem]
          [org.jf.dexlib2.iface MethodParameter]
          [java.util Arrays]
          [java.util.jar Manifest]
          [java.io File RandomAccessFile]))

(set! *warn-on-reflection* true)

(defmacro rangelist
  "(for [idx (range foo)] bar) -> (rangelist [idx foo] bar). strict."
  [[binder cnt] & generator]
  `(let [cc# ~cnt
         res# (object-array cc#)]
    (dotimes [~binder cc#]
      (aset res# ~binder (do ~@generator)))
    (Arrays/asList res#)))

(defn get-manifest
  [^AbstractApkFile apk]
  (->
    (.getFileData apk "META-INF/MANIFEST.MF")
    (io/input-stream)
    (Manifest.)
    (.getEntries)))

(defn file-from-bytes
  [^bytes bs]
  (let [res (File/createTempFile "temp" "blob")]
    (with-open [outs (io/output-stream res)]
      (.write outs bs))
    res))

(defn ra-file-from-bytes
  [^bytes bs]
  (let [^File fd (file-from-bytes bs)]
    [fd (RandomAccessFile. fd "a+")]))

(defmacro with-ra-file
  [[binder inp] & bodies]
  `(let [[^File fd# ~binder] (ra-file-from-bytes ~inp)]
    (try
      ~@bodies
      (finally
        (.delete fd#)))))

(defn mask-flags
  [v & pairs]
  (reduce
    (fn [res [k mask]]
      (if (zero? (bit-and mask v))
        res
        (conj res k)))
    #{}
    (partition 2 pairs)))

(defn dex-access
  [v]
  ;; https://www.cs.umd.edu/projects/PL/redexer/doc/Dex.html
  (mask-flags v
    :PUBLIC 		0x1
    :PRIVATE 		0x2
    :PROTECTED 		0x4
    :STATIC 		0x8
    :FINAL 		    0x10
    :SYNCHRONIZED 	0x20
    :VOLATILE 		0x40
    :BRIDGE 		0x40
    :TRANSIENT 		0x80
    :VARARGS 		0x80
    :NATIVE 		0x100
    :INTERFACE 		0x200
    :ABSTRACT 		0x400
    :STRICT 		0x800
    :SYNTHETIC 		0x1000
    :ANNOTATION 	0x2000
    :ENUM 		    0x4000
    :CONSTRUCTOR 	0x10000
    :DECLARED_SYNCHRONIZED 		0x20000))

(defn dex-visibility
  [v]
  (case (int v)
    0x00 :build
    0x01 :runtime
    0x02 :system))
   
(defn dex-annotation
  [^DexBackedAnnotation ann]
  {:visibility (dex-visibility (.getVisibility ann))
   :type (.getType ann)
   :elements (into {} (for [^DexBackedAnnotationElement e (.getElements ann)] [(.getName e) (.getValue e)]))})

(defn dex-encoded-value
  [^EncodedValue v]
  (if (nil? v)
    nil
    (let [t (.getValueType v)]
      (if (nil? t)
        nil
        (case t 
          ValueType/BYTE :BYTE
          ValueType/SHORT :SHORT
          ValueType/CHAR :CHAR
          ValueType/INT :INT
          ValueType/LONG :LONG
          ValueType/FLOAT :FLOAT
          ValueType/DOUBLE :DOUBLE
          ValueType/STRING :STRING
          ValueType/TYPE :TYPE
          ValueType/FIELD :FIELD
          ValueType/METHOD :METHOD
          ValueType/ENUM :ENUM
          ValueType/ARRAY :ARRAY
          ValueType/ANNOTATION :ANNOTATION
          ValueType/NULL :NULL
          ValueType/BOOLEAN :BOOLEAN
          nil)))))

(defn dex-field
  [^DexBackedField f]
  {:name (.getName f)
   :type (.getType f)
   :defining_class (.getDefiningClass f)
   :annotations (map dex-annotation (.getAnnotations f))
   :initial_value (dex-encoded-value (.getInitialValue f))})

(defn dex-method-parameter
  [^MethodParameter p]
  {:type (.getType p)
   :annotations (map dex-annotation (.getAnnotations p))
   :name (.getName p)
   :signature (.getSignature p)})

(defn dex-debug-item
  [^DebugItem d]
  {:type (.getDebugItemType d)
   :addr (.getCodeAddress d)})

(def dex-instruction bean)

(defn dex-method-impl
  [^DexBackedMethodImplementation m]
  (if (nil? m)
    nil
    {:register-count (.getRegisterCount m)
     :debug (map dex-debug-item (.getDebugItems m))
     :instructions (map dex-instruction (.getInstructions m))}))

(defn dex-method
  [^DexBackedMethod m]
  {:index (.getMethodIndex m)
   :defining_class (.getDefiningClass m)
   :access (dex-access (.getAccessFlags m))
   :name (.getName m)
   :return_type (.getReturnType m)
   :parameters (map dex-method-parameter (.getParameters m))
   :annotations (map dex-annotation (.getAnnotations m))
   :impl (dex-method-impl (.getImplementation m))})

(defn dex-class
  [^DexBackedClassDef c]
  {:type (.getType c)
   :superclass (.getSuperclass c)
   :access (dex-access (.getAccessFlags c))
   :source_file (.getSourceFile c)
   :interfaces (.getInterfaces c)
   :annotations (map dex-annotation (.getAnnotations c))
   :static_fields (map dex-field (.getStaticFields c))
   :instance_fields (map dex-field (.getInstanceFields c))
   :direct_methods (map dex-method (.getDirectMethods c))
   :virtual_methods (map dex-method (.getVirtualMethods c))})

(defn get-dex
  [^AbstractApkFile apk]
  (let [^bytes data (.getFileData apk AndroidConstants/DEX_FILE)
        ^DexBackedDexFile dex (DexBackedDexFile. (Opcodes/getDefault) data)]
    {:classes (map dex-class (.getClasses dex))}))

(defn load-apk
  [^bytes apk-data]
  (let [^File fd (file-from-bytes apk-data)]
    (let [veri (->
                  (ApkVerifier$Builder. fd)
                  (.build)
                  (.verify))
          apk (ByteArrayApkFile. apk-data)]
      (.delete fd)
      {:apk apk 
       :manifest (get-manifest apk)
       :dex (get-dex apk)
       :verification veri})))
