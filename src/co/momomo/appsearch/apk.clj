(ns co.momomo.appsearch.apk
  (require [clojure.java.io :as io]
           [clojure.data.xml :as xml])
  (import [net.dongliu.apk.parser ApkFile ByteArrayApkFile]
          [net.dongliu.apk.parser.struct AndroidConstants]
          [com.android.apksig ApkVerifier ApkVerifier$Builder]
          [hu.uw.pallergabor.dedexer
            Dedexer DexSignatureBlock DexDependencyParser
            DexPointerBlock DexStringIdsBlock DexTypeIdsBlock
            DexFieldIdsBlock DexMethodIdsBlock DexClassDefsBlock
            DexProtoIdsBlock Annotation AnnotationHolder
            DexAnnotationParser DexAnnotationParser$AnnotationType]
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
  [^ApkFile apk]
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

(defn get-visibility
  [v]
  (case v
    DexAnnotationParser/VISIBILITY_BUILD :build
    DexAnnotationParser/VISIBILITY_RUNTIME :runtime
    DexAnnotationParser/VISIBILITY_SYSTEM :system
    nil))
  
(defn parse-annotation-holder
  [^AnnotationHolder h]
  (for [^Annotation a (.annotations h)]
    {:visibility (get-visibility (.visibility a))
     :type (.type a)
     :elements (array-map (map (fn [a b] [a b])
                            (.elementNames a) (.elementValues a)))}))

(defn get-annotations
  [^DexClassDefsBlock dcb cls]
  (let [dap (.getDexAnnotationParser dcb cls)]
    {:fields (doall (map parse-annotation-holder (.getFieldAnnotations dap)))
     :methods (doall (map parse-annotation-holder (.getMethodAnnotations dap)))
     :parameters (doall (map parse-annotation-holder (.getParameterAnnotations dap)))
     :class (doall (map parse-annotation-holder (.getClassAnnotations dap)))}))

(defmacro dexconstruct
  [classname a b]
  `(doto (~classname)
    (.setDexSignatureBlock ~a)
    (.setRandomAccessFile ~b)
    (.setDumpFile nil)
    (.parse)))

(defn method-body
  [& args]
  nil)

(defn get-dex
  [^ApkFile apk]
  (with-ra-file [rfd (.getFileData apk AndroidConstants/DEX_FILE)]
    (let [dsb (doto (DexSignatureBlock.)
                    (.setRandomAccessFile rfd)
                    (.setDumpFile nil)
                    (.parse))
          deps-parser (dexconstruct DexDependencyParser. dsb rfd)
          dpb (dexconstruct DexPointerBlock. dsb rfd)
          dstrb (dexconstruct DexStringIdsBlock. dsb rfd)
          dtb (dexconstruct DexTypeIdsBlock. dsb rfd)
          dpib (dexconstruct DexProtoIdsBlock. dsb rfd)
          dfb (dexconstruct DexFieldIdsBlock. dsb rfd)
          dmb (dexconstruct DexMethodIdsBlock. dsb rfd)
          dcb (dexconstruct DexClassDefsBlock. dsb rfd)]
        (doall
          (for [^Integer cls (iterator-seq (.getClassIterator dcb))]
            {:class_name (.getClassNameOnly dcb cls)
             :interface? (.isInterface dcb cls)
             :superclass_name (.getSuperClass dcb cls)
             :source_name (.getSourceName dcb cls)
             :implements (rangelist [idx (.getInterfacesSize dcb)]
                          (.getInterface dcb cls idx))
             :annotations (get-annotations dcb cls)
             :static_fields (rangelist [idx (.getStaticFieldsSize dcb cls)]
                              (let [sn (.getStaticFieldShortName dcb cls idx)]
                                {:name (.getStaticField dcb cls idx)
                                 :initializer (.getStaticFieldInitializer dcb cls idx)
                                 :short_name sn}))
             :instance_fields (rangelist [idx (.getInstanceFieldSize dcb cls)]
                                (let [sn (.getFieldShortName dcb cls idx)]
                                  {:name (.getInstanceField dcb cls idx)
                                   :short_name sn}))
             :direct_methods (rangelist [idx (.getDirectMethodsFieldsSize dcb cls)]
                              (let [sn (.getDirectMethodShortName dcb cls idx)
                                    n (.getDirectMethodName dcb cls idx)]  
                                {:name n 
                                 :short_name sn
                                 :access (.getDirectMethodAccess dcb cls idx)
                                 :body (method-body dcb n (.getDirectMethodOffset dcb cls idx) cls idx)}))
             :virtual_methods (rangelist [idx (.getVirtualMehtodsFieldsSize dcb cls)]
                                (let [n (.getVirtualMethodName dcb cls idx)
                                      sn (.getVirtualShortMethodName dcb cls idx)]
                                  {:name n
                                   :short_name sn
                                   :access (.getVirtualMethodAccess dcb cls idx)
                                   :body (method-body dcb n (.getVirtualMethodOffset dcb cls idx) cls idx)}))})))))


(defn load-apk
  [^bytes apk-data]
  (let [fd (file-from-bytes apk-data)]
    (let [veri (->
                  (ApkVerifier$Builder. fd)
                  (.setMinSdkVersion 14)
                  (.setMinCheckedPlatformVersion 14)
                  (.build)
                  (.verify))
          apk (ByteArrayApkFile. apk-data)]
      (.delete fd)
      {:apk apk 
       :manifest (get-manifest apk)
       :dex (get-dex apk)
       :verification veri})))
