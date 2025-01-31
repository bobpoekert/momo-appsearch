(ns co.momomo.appsearch.apk
  (require [clojure.java.io :as io]
           [proteus :refer [let-mutable]])
  (import [net.dongliu.apk.parser ApkFile AbstractApkFile ByteArrayApkFile]
          [net.dongliu.apk.parser.struct AndroidConstants]
          [com.android.apksig ApkVerifier ApkVerifier$Builder]
          [com.android.apksig.util DataSources]
          [org.jf.dexlib2 Opcode Opcodes ValueType]
          [org.jf.dexlib2.dexbacked DexBackedDexFile
            DexBackedAnnotation DexBackedAnnotationElement DexBackedField
            DexBackedMethodImplementation DexBackedMethod DexBackedClassDef]
          [org.jf.dexlib2.iface.value EncodedValue]
          [org.jf.dexlib2.iface.debug DebugItem]
          [org.jf.dexlib2.iface MethodParameter]
          [org.jf.dexlib2.iface.instruction Instruction
            DualReferenceInstruction FieldOffsetInstruction
            FiveRegisterInstruction HatLiteralInstruction
            InlineIndexInstruction OffsetInstruction
            OneRegisterInstruction ReferenceInstruction
            RegisterRangeInstruction SwitchElement SwitchPayload
            ThreeRegisterInstruction TwoRegisterInstruction
            VariableRegisterInstruction VerificationErrorInstruction
            VtableIndexInstruction WideLiteralInstruction]
          [brut.androlib.res.decoder ARSCDecoder]
          [brut.androlib.res.data ResType ResPackage ResValuesFile ResTypeSpec ResResource]
          [brut.androlib.res.xml ResXmlEncodable]
          [java.util Arrays HashMap Collections ArrayList]
          [java.util.jar Manifest]
          [java.nio ByteBuffer]
          [java.io File RandomAccessFile ByteArrayInputStream InputStream ByteArrayOutputStream]
          [java.util.zip ZipInputStream ZipEntry ZipException]))

(set! *warn-on-reflection* true)
   
(defn hash-instruction
  [^Instruction v]
  (let [^Opcode op (.getOpcode v)]
    (let-mutable [res -1]
      (set! res (hash-combine res (hash (.getCodeUnits v))))
      (set! res (hash-combine res (hash (.name op))))
      (set! res (hash-combine res (hash (.flags op))))
      (set! res (hash-combine res (hash (.referenceType op))))
      (set! res (hash-combine res (hash (.referenceType2 op))))
      (when (instance? DualReferenceInstruction v)
        (set! res (hash-combine res (hash (.getReferenceType2 ^DualReferenceInstruction v))))
        (set! res (hash-combine res (.hashCode (.getReference2 ^DualReferenceInstruction v)))))
      (when (instance? FieldOffsetInstruction v)
        (set! res (hash-combine res (hash (.getFieldOffset ^FieldOffsetInstruction v)))))
      (when (instance? FiveRegisterInstruction v)
        (set! res (hash-combine res (hash (.getRegisterC ^FiveRegisterInstruction v))))
        (set! res (hash-combine res (hash (.getRegisterD ^FiveRegisterInstruction v))))
        (set! res (hash-combine res (hash (.getRegisterE ^FiveRegisterInstruction v))))
        (set! res (hash-combine res (hash (.getRegisterF ^FiveRegisterInstruction v))))
        (set! res (hash-combine res (hash (.getRegisterG ^FiveRegisterInstruction v)))))
      (when (instance? HatLiteralInstruction v)
        (set! res (hash-combine res (hash (.getHatLiteral ^HatLiteralInstruction v)))))
      (when (instance? InlineIndexInstruction v)
        (set! res (hash-combine res (hash (.getInlineIndex ^InlineIndexInstruction v)))))
      (when (instance? OffsetInstruction v)
        (set! res (hash-combine res (hash (.getCodeOffset ^OffsetInstruction v)))))
      (when (instance? OneRegisterInstruction v)
        (set! res (hash-combine res (hash (.getRegisterA ^OneRegisterInstruction v)))))
      (when (instance? ReferenceInstruction v)
        (set! res (hash-combine res (hash (.getReferenceType ^ReferenceInstruction v))))
        (set! res (hash-combine res (.hashCode (.getReference ^ReferenceInstruction v)))))
      (when (instance? RegisterRangeInstruction v)
        (set! res (hash-combine res (hash (.getStartRegister ^RegisterRangeInstruction v)))))
      (when (instance? SwitchPayload v)
        (doseq [^SwitchElement elem (.getSwitchElements ^SwitchPayload v)]
          (set! res (hash-combine res (hash (.getKey elem))))
          (set! res (hash-combine res (hash (.getOffset elem))))))
      (when (instance? ThreeRegisterInstruction v)
        (set! res (hash-combine res (hash (.getRegisterC ^ThreeRegisterInstruction v)))))
      (when (instance? TwoRegisterInstruction v)
        (set! res (hash-combine res (hash (.getRegisterB ^TwoRegisterInstruction v)))))
      (when (instance? VariableRegisterInstruction v)
        (set! res (hash-combine res (hash (.getRegisterCount ^VariableRegisterInstruction v)))))
      (when (instance? VerificationErrorInstruction v)
        (set! res (hash-combine res (hash (.getVerificationError ^VerificationErrorInstruction v)))))
      (when (instance? VtableIndexInstruction v)
        (set! res (hash-combine res (hash (.getVtableIndex ^VtableIndexInstruction v)))))
      (when (instance? WideLiteralInstruction v)
        (set! res (hash-combine res (hash (.getWideLiteral ^WideLiteralInstruction v)))))
      res)))

(defmacro rangelist
  "(for [idx (range foo)] bar) -> (rangelist [idx foo] bar). strict."
  [[binder cnt] & generator]
  `(let [cc# ~cnt
         res# (object-array cc#)]
    (dotimes [~binder cc#]
      (aset res# ~binder (do ~@generator)))
    (Arrays/asList res#)))

(defn parse-manifest-entry
  [e]
  (into {}
    (for [[k v] e]
      [(str k) v])))

(defn get-manifest
  [^AbstractApkFile apk]
  (when-let [data (.getFileData apk "META-INF/MANIFEST.MF")]
    (->>
      (->
        data 
        (io/input-stream)
        (Manifest.)
        (.getEntries))
      (map (fn [v] [(key v) (parse-manifest-entry (val v))]))
      (into {}))))

(defn mask-flags
  [v & pairs]
  (reduce
    (fn [res [k mask]]
      `(if (zero? (bit-and ~mask ~v))
        ~res
        (cons ~k ~res)))
    '()
    (partition 2 pairs)))

(defn dex-access
  [v]
  ;; https://www.cs.umd.edu/projects/PL/redexer/doc/Dex.html
  (delay
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
      :DECLARED_SYNCHRONIZED 		0x20000)))

(defn dex-visibility
  [v]
  (case (int v)
    0x00 :build
    0x01 :runtime
    0x02 :system))
   
(defn dex-annotation
  [^DexBackedAnnotation ann]
  {:visibility (dex-visibility (.getVisibility ann))
   :type (str (.getType ann))
   :elements (into {} (for [^DexBackedAnnotationElement e (.getElements ann)] [(str (.getName e)) (str (.getValue e))]))})

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
  {:name (str (.getName f))
   :type (str (.getType f))
   :defining_class (str (.getDefiningClass f))
   :annotations (map dex-annotation (.getAnnotations f))
   :initial_value (dex-encoded-value (.getInitialValue f))})

(defn dex-method-parameter
  [^MethodParameter p]
  {:type (str (.getType p))
   :annotations (map dex-annotation (.getAnnotations p))
   :name (str (.getName p))
   :signature (str (.getSignature p))})

(defn dex-debug-item
  [^DebugItem d]
  {:type (str (.getDebugItemType d))
   :addr (int (.getCodeAddress d))})

(defn dex-method-impl
  [^DexBackedMethodImplementation m]
  (if (nil? m)
    nil
    {:register-count (int (.getRegisterCount m))
     :debug (map dex-debug-item (.getDebugItems m))
     :code_hash (reduce hash-combine (map hash-instruction (.getInstructions m)))}))

(defn dex-method
  [^DexBackedMethod m]
  {:index (int (.getMethodIndex m))
   :defining_class (str (.getDefiningClass m))
   :access (dex-access (.getAccessFlags m))
   :name (str (.getName m))
   :return_type (str (.getReturnType m))
   :parameters (map dex-method-parameter (.getParameters m))
   :annotations (map dex-annotation (.getAnnotations m))
   :impl (dex-method-impl (.getImplementation m))})

(defn dex-class
  [^DexBackedClassDef c]
  {:type (str (.getType c))
   :superclass (str (.getSuperclass c))
   :access (dex-access (.getAccessFlags c))
   :source_file (str (.getSourceFile c))
   :interfaces (str (.getInterfaces c))
   :annotations (map dex-annotation (.getAnnotations c))
   :static_fields (map dex-field (.getStaticFields c))
   :instance_fields (map dex-field (.getInstanceFields c))
   :direct_methods (map dex-method (.getDirectMethods c))
   :virtual_methods (map dex-method (.getVirtualMethods c))})

(defn ^DexBackedDexFile get-dex
  [^AbstractApkFile apk]
  (let [^bytes data (.getFileData apk AndroidConstants/DEX_FILE)
        ^DexBackedDexFile dex (DexBackedDexFile. (Opcodes/getDefault) data)]
    dex))

(defn parse-cert
  [^java.security.cert.X509Certificate c]
  {:subject (.getName (.getSubjectX500Principal c))
   :issuer (.getName (.getIssuerX500Principal c))
   :not_after (.getTime (.getNotAfter c))
   :not_before (.getTime (.getNotBefore c))
   :algo (.getAlgorithm (.getPublicKey c))
   :key_format (.getFormat (.getPublicKey c))
   :key (.getEncoded (.getPublicKey c))})

(defn parse-issue
  [^com.android.apksig.ApkVerifier$IssueWithParams w]
  {:name (str (.getIssue w))
   :params (map str (seq (.getParams w)))})

(defn parse-verification
  [^com.android.apksig.ApkVerifier$Result v]
  {:verified (.isVerified v)
   :certs (map parse-cert (.getSignerCertificates v))
   :warnings (map parse-issue (seq (.getWarnings v)))
   :errors (map parse-issue (seq (.getErrors v)))})

(defn verify
  [^bytes apk-data]
  (->
    apk-data
    (ByteBuffer/wrap)
    (DataSources/asDataSource)
    (ApkVerifier$Builder.)
    (.build)
    (.verify)
    (parse-verification)))

(defn load-apk
  [^bytes apk-data]
  (let [apk (ByteArrayApkFile. apk-data)
        dex (get-dex apk)]
    {:manifest (get-manifest apk)
     :dex dex
     :classes (map dex-class (.getClasses dex))
     :manifest_xml (.getManifestXml apk)
     :verification (delay (verify apk-data))}))

(defn get-zip-data
  [^ZipInputStream zi]
  (let [res (ByteArrayOutputStream.)
        buf (byte-array 1024)]
    (loop [len (.read zi buf 0 1024)]
      (if (= -1 len)
        (.toByteArray res)
        (do
          (.write res buf 0 len)
          (recur (.read zi buf 0 1024)))))))

(defprotocol ExtractFile
  (extract-file [v k]))

(defn get-zip-entry
  [^ZipInputStream zi]
  (try
    (.getNextEntry zi)
    (catch ZipException e :fail)))

(extend-protocol ExtractFile
  AbstractApkFile
  (extract-file [v k]
    (.getFileData v k))
  InputStream
  (extract-file [v k]
    (let [zi (ZipInputStream. v)]
      (loop [entry (get-zip-entry zi)]
        (cond
          (nil? entry) nil
          (= entry :fail) (recur (get-zip-entry zi))
          (= (.getName ^ZipEntry entry) k) (get-zip-data zi)
          :else (recur (get-zip-entry zi)))))))

(defn get-resources
  [apk]
  (when-let [v (extract-file apk "resources.arsc")]
    (->
      (ARSCDecoder/decode
        (ByteArrayInputStream. ^bytes v)
        true true)
       (.getPackages))))

(defmacro doarr
  [[bind arr] & bodies]
  `(dotimes [i# (alength ~arr)]
    (let [~bind (aget ~arr i#)]
      ~@bodies)))

(defn get-strings
  [apk]
  (let [^objects resources (get-resources apk)
        res (ArrayList.)]
    (when-not (nil? resources)
      (doarr [^ResPackage p resources]
        (doseq [^ResValuesFile fd (.listValuesFiles p)]
          (let [^ResTypeSpec typ (.getType fd)
                locale (.getQualifiers (.getFlags (.getConfig fd)))]
            (when (.isString typ)
              (doseq [^ResResource rs (.listResources fd)]
                (let [k (.getName (.getResSpec rs))
                      v (.encodeAsResXmlValue ^ResXmlEncodable (.getValue rs))]
                  (.add res [k v locale]))))))))
      res))

(defn histogram
  [vs]
  (let [^HashMap res (HashMap.)]
    (doseq [v vs]
      (if (.containsKey res v)
        (.put res v (inc (.get res v)))
        (.put res v 1)))
    (Collections/unmodifiableMap res)))

(defmacro doiter
  [[binder iter] & bodies]
  `(when-not (nil? ~iter)
    (let [^java.util.Iterator it# (.iterator ^Iterable ~iter)]
      (while (.hasNext it#)
        (let [~binder (.next it#)]
          ~@bodies)))))

(defn hist-add
  [^HashMap m v]
  (when-not (nil? v)
    (if (.containsKey m v)
      (.put m v (inc (.get m v)))
      (.put m v 1))))

(defn hash-instructions
  [^DexBackedMethod m]
  (let [v (.getImplementation m)]
    (if (nil? v)
      nil
      (let [v (.getInstructions v)
           ^java.util.Iterator it (.iterator v)]
        (loop [res -1]
          (if (.hasNext it)
            (let [v (.next it)]
              (if (nil? v)
                (recur res)
                (recur (hash-combine res (hash-instruction v)))))
            res))))))

(defn method-hashes
  [^bytes apk-data]
  (let [^HashMap res (HashMap.)
        dex (get-dex (ByteArrayApkFile. apk-data))]
    (doiter [^DexBackedClassDef cls (.getClasses dex)]
      (doiter [^DexBackedMethod m (.getDirectMethods cls)]
        (hist-add res (hash-instructions m)))
      (doiter [^DexBackedMethod m (.getVirtualMethods cls)]
        (hist-add res (hash-instructions m))))
   res)) 

