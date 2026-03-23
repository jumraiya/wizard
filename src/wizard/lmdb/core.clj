(ns wizard.lmdb.core
  (:import
   (org.lmdbjava Env Dbi DbiFlags EnvFlags GetOp PutFlags SeekOp Txn)
   (java.nio ByteBuffer ByteOrder)
   (java.io File)
   (java.nio.charset StandardCharsets)
   (java.util Arrays)))

;; Type tags - matching datalevin/bits.clj conventions for sort-order compatibility.
;; Fixed-size types (bool: 2B, long: 9B, float: 5B, double: 9B) need no terminator.
;; Variable-size types (string, keyword, symbol) are 0x00-terminated.
(def ^:private ^:const type-long-neg (unchecked-byte 0xC0)) ; negative long
(def ^:private ^:const type-long-pos (unchecked-byte 0xC1)) ; non-negative long
(def ^:private ^:const type-float    (unchecked-byte 0xF5))
(def ^:private ^:const type-double   (unchecked-byte 0xF6))
(def ^:private ^:const type-string   (unchecked-byte 0xFA))
(def ^:private ^:const type-keyword  (unchecked-byte 0xFB))
(def ^:private ^:const type-symbol   (unchecked-byte 0xFC))
(def ^:private ^:const type-boolean  (unchecked-byte 0xFD))
(def ^:private ^:const val-false     (unchecked-byte 0x01))
(def ^:private ^:const val-true      (unchecked-byte 0x02))
(def ^:private ^:const separator     (unchecked-byte 0x00))

;; ---- Per-element encoding helpers ----
;; All use ByteBuffer (big-endian by default) for correct bit representation.
;; Order-preserving trick for longs/doubles/floats: XOR the sign bit so that
;; the unsigned byte order matches the signed numeric order.

(defn- long->bytes ^bytes [^long n]
  (let [enc (bit-xor n Long/MIN_VALUE)
        ba  (byte-array 8)]
    (.putLong (ByteBuffer/wrap ba) enc)
    ba))

(defn- bytes->long [^bytes ba off]
  (let [enc (.getLong (ByteBuffer/wrap ba (int off) 8))]
    (bit-xor enc Long/MIN_VALUE)))

(defn- float->bytes ^bytes [x]
  (let [bits (Float/floatToIntBits x)
        enc  (unchecked-int (if (neg? bits) (bit-not bits) (bit-xor bits Integer/MIN_VALUE)))
        ba   (byte-array 4)]
    (.putInt (ByteBuffer/wrap ba) enc)
    ba))

(defn- bytes->float [^bytes ba off]
  (let [enc  (.getInt (ByteBuffer/wrap ba (int off) 4))
        bits (unchecked-int
              (if (neg? enc)
                (bit-xor enc Integer/MIN_VALUE)
                (bit-not enc)))]
    (Float/intBitsToFloat bits)))

(defn- double->bytes ^bytes [^double x]
  (let [bits (Double/doubleToLongBits x)
        enc  (if (neg? bits) (bit-not bits) (bit-xor bits Long/MIN_VALUE))
        ba   (byte-array 8)]
    (.putLong (ByteBuffer/wrap ba) enc)
    ba))

(defn- bytes->double [^bytes ba off]
  (let [enc  (.getLong (ByteBuffer/wrap ba (int off) 8))
        bits (if (neg? enc) (bit-xor enc Long/MIN_VALUE) (bit-not enc))]
    (Double/longBitsToDouble bits)))

(defn- kw-sym-str ^String [x]
  (if-let [ns (namespace x)]
    (str ns "/" (name x))
    (name x)))

(defn- str->kw [^String s]
  (let [slash (.indexOf s (int \/))]
    (if (= slash -1) (keyword s) (keyword (subs s 0 slash) (subs s (inc slash))))))

(defn- str->sym [^String s]
  (let [slash (.indexOf s (int \/))]
    (if (= slash -1) (symbol s) (symbol (subs s 0 slash) (subs s (inc slash))))))

;; ---- Encode / decode a single value ----

(defn- encode-val ^bytes [v]
  (cond
    (boolean? v)
    (byte-array [type-boolean (if v val-true val-false)])

    (int? v)
    (let [n   (long v)
          lb  (long->bytes n)
          ba  (byte-array 9)]
      (aset-byte ba 0 (if (neg? n) type-long-neg type-long-pos))
      (System/arraycopy lb 0 ba 1 8)
      ba)

    (instance? Float v)
    (let [fb (float->bytes v)
          ba (byte-array 5)]
      (aset-byte ba 0 type-float)
      (System/arraycopy fb 0 ba 1 4)
      ba)

    (float? v)
    (let [db (double->bytes (double v))
          ba (byte-array 9)]
      (aset-byte ba 0 type-double)
      (System/arraycopy db 0 ba 1 8)
      ba)

    (string? v)
    (let [sb (.getBytes ^String v StandardCharsets/UTF_8)
          n  (alength sb)
          ba (byte-array (+ 2 n))]
      (aset-byte ba 0 type-string)
      (System/arraycopy sb 0 ba 1 n)
      (aset-byte ba (inc n) separator)
      ba)

    (keyword? v)
    (let [sb (.getBytes ^String (kw-sym-str v) StandardCharsets/UTF_8)
          n  (alength sb)
          ba (byte-array (+ 2 n))]
      (aset-byte ba 0 type-keyword)
      (System/arraycopy sb 0 ba 1 n)
      (aset-byte ba (inc n) separator)
      ba)

    (symbol? v)
    (let [sb (.getBytes ^String (kw-sym-str v) StandardCharsets/UTF_8)
          n  (alength sb)
          ba (byte-array (+ 2 n))]
      (aset-byte ba 0 type-symbol)
      (System/arraycopy sb 0 ba 1 n)
      (aset-byte ba (inc n) separator)
      ba)

    :else
    (throw (ex-info "Unsupported type for LMDB encoding"
                    {:value v :type (type v)}))))

;; ---- Public encode / decode ----

(defn encode
  "Encode a vector of Clojure values into a byte array suitable for use as an
  LMDB key or value.  Each element is type-tagged; fixed-size types (boolean,
  long, float, double) are self-delimiting; variable-size types (string,
  keyword, symbol) are 0x00-terminated.  Because the encoding is self-
  delimiting, encode([v1 v2]) bytes are always a byte-level prefix of
  encode([v1 v2 v3 …]), so prefix-search works naturally."
  ^bytes [v]
  (let [parts (mapv encode-val v)
        total (transduce (map alength) + 0 parts)
        ba    (byte-array total)]
    (loop [parts parts pos 0]
      (if (empty? parts)
        ba
        (let [p (first parts)
              n (alength p)]
          (System/arraycopy p 0 ba pos n)
          (recur (rest parts) (+ pos n)))))))

(defn decode
  "Decode a byte array produced by `encode` back into a vector of Clojure values."
  [^bytes ba]
  (let [n (alength ba)]
    (loop [pos 0 result (transient [])]
      (if (>= pos n)
        (persistent! result)
        (let [tag (aget ba pos)]
          (condp = tag
            type-boolean
            (recur (+ pos 2)
                   (conj! result (= (aget ba (inc pos)) val-true)))

            type-long-neg
            (recur (+ pos 9) (conj! result (bytes->long ba (inc pos))))

            type-long-pos
            (recur (+ pos 9) (conj! result (bytes->long ba (inc pos))))

            type-float
            (recur (+ pos 5) (conj! result (bytes->float ba (inc pos))))

            type-double
            (recur (+ pos 9) (conj! result (bytes->double ba (inc pos))))

            type-string
            (let [end (loop [i (inc pos)]
                        (if (= (aget ba i) separator) i (recur (inc i))))
                  s   (String. ba (inc pos) (- end pos 1) StandardCharsets/UTF_8)]
              (recur (inc end) (conj! result s)))

            type-keyword
            (let [end (loop [i (inc pos)]
                        (if (= (aget ba i) separator) i (recur (inc i))))
                  s   (String. ba (inc pos) (- end pos 1) StandardCharsets/UTF_8)]
              (recur (inc end) (conj! result (str->kw s))))

            type-symbol
            (let [end (loop [i (inc pos)]
                        (if (= (aget ba i) separator) i (recur (inc i))))
                  s   (String. ba (inc pos) (- end pos 1) StandardCharsets/UTF_8)]
              (recur (inc end) (conj! result (str->sym s))))

            (throw (ex-info "Unknown type tag during LMDB decode"
                            {:tag tag :pos pos}))))))))

;; ---- LMDB plumbing ----

(defn- ->direct-buf ^ByteBuffer [^bytes bs]
  (doto (ByteBuffer/allocateDirect (alength bs))
    (.put bs)
    (.flip)))

(defn- buf->bytes ^bytes [^ByteBuffer buf]
  (let [bs (byte-array (.remaining buf))]
    (.get buf bs)
    bs))

(defn open-db
  "Open (or create) a named LMDB database at dir (a path string or java.io.File).
   Returns a map with :env and :db keys."
  [dir ^String db-name]
  (let [env (-> (Env/create)
                (.setMapSize 10485760000)
                (.setMaxDbs 20)
                (.open (File. ^String (str dir)) (make-array EnvFlags 0)))
        db  (.openDbi env db-name (into-array DbiFlags [DbiFlags/MDB_CREATE]))]
    {:env env :db db}))

;; ---- Transaction-aware operations ----
;; Use these when batching many reads/writes into one transaction.
;; Callers are responsible for opening and committing/aborting the transaction.

(defn write-txn
  "Open a write transaction. Must be closed with (.commit txn) or (.abort txn)."
  ^Txn [{:keys [env]}]
  (.txnWrite env))

(defn put-txn
  "Store key-vec → boolean val within an existing transaction."
  [{:keys [^Dbi db]} ^Txn txn key-vec val]
  (let [key-buf (->direct-buf (encode key-vec))
        val-buf (->direct-buf (encode-val val))]
    (.put db txn key-buf val-buf (make-array PutFlags 0))))

(defn get-val-txn
  "Look up key-vec within an existing transaction; returns the boolean value or nil."
  [{:keys [^Dbi db]} ^Txn txn key-vec]
  (let [key-buf (->direct-buf (encode key-vec))]
    (when-let [^ByteBuffer val-buf (.get db txn key-buf)]
      (first (decode (buf->bytes val-buf))))))

(defn delete-txn
  "Delete the entry for key-vec within an existing transaction."
  [{:keys [^Dbi db]} ^Txn txn key-vec]
  (.delete db txn (->direct-buf (encode key-vec))))

(defn prefix-search-txn
  "Return all [key-vec val] pairs whose encoded key starts with encode(prefix-vec),
  within an existing transaction."
  [{:keys [^Dbi db]} ^Txn txn prefix-vec]
  (let [prefix-bytes (encode prefix-vec)
        key-buf      (->direct-buf prefix-bytes)]
    (with-open [cursor (.openCursor db txn)]
      (if (.get cursor key-buf GetOp/MDB_SET_RANGE)
        (loop [results []]
          (let [k-bs (buf->bytes (.key cursor))]
            (if (and (>= (alength k-bs) (alength prefix-bytes))
                     (Arrays/equals prefix-bytes 0 (alength prefix-bytes)
                                    k-bs         0 (alength prefix-bytes)))
              (let [entry [(decode k-bs) (first (decode (buf->bytes (.val cursor))))]]
                (if (.seek cursor SeekOp/MDB_NEXT)
                  (recur (conj results entry))
                  (conj results entry)))
              results)))
        []))))

;; ---- Single-operation convenience functions ----
;; Each opens and commits its own transaction. Use the -txn variants above
;; when issuing many operations to avoid per-call transaction overhead.

(defn put
  "Store key-vec → boolean val."
  [{:keys [env] :as ctx} key-vec val]
  (with-open [^Txn txn (.txnWrite env)]
    (put-txn ctx txn key-vec val)
    (.commit txn)))

(defn get-val
  "Look up key-vec; returns the boolean value, or nil if absent."
  [{:keys [env] :as ctx} key-vec]
  (with-open [^Txn txn (.txnRead env)]
    (get-val-txn ctx txn key-vec)))

(defn prefix-search
  "Return all [key-vec val] pairs whose encoded key starts with encode(prefix-vec)."
  [{:keys [env] :as ctx} prefix-vec]
  (with-open [^Txn txn (.txnRead env)]
    (prefix-search-txn ctx txn prefix-vec)))

(defn delete
  "Delete the entry for key-vec."
  [{:keys [env] :as ctx} key-vec]
  (with-open [^Txn txn (.txnWrite env)]
    (delete-txn ctx txn key-vec)
    (.commit txn)))
