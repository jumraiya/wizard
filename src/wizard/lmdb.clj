(ns wizard.lmdb
  (:require [clojure.edn :as edn])
  (:import
   (org.lmdbjava Env Dbi DbiFlags EnvFlags GetOp PutFlags SeekOp Txn)
   (java.nio ByteBuffer)
   (java.nio.charset StandardCharsets)
   (java.util Arrays)))


;; Encoding scheme: first byte is a type tag so decode is unambiguous,
;; and string keys are stored as raw UTF-8 bytes (no pr-str quoting) so
;; lexicographic prefix search works naturally.
;;   0x01 = String  → raw UTF-8 bytes
;;   0x02 = Other   → pr-str UTF-8 bytes (keywords, numbers, vectors, maps, …)

(defn- encode ^bytes [v]
  (if (string? v)
    (let [raw (.getBytes ^String v StandardCharsets/UTF_8)
          buf (byte-array (inc (alength raw)))]
      (aset-byte buf 0 1)
      (System/arraycopy raw 0 buf 1 (alength raw))
      buf)
    (let [raw (.getBytes (pr-str v) StandardCharsets/UTF_8)
          buf (byte-array (inc (alength raw)))]
      (aset-byte buf 0 2)
      (System/arraycopy raw 0 buf 1 (alength raw))
      buf)))

(defn- decode [^bytes bs]
  (let [tag  (aget bs 0)
        rest (Arrays/copyOfRange bs 1 (alength bs))]
    (case tag
      1 (String. rest StandardCharsets/UTF_8)
      2 (edn/read-string (String. rest StandardCharsets/UTF_8)))))

(defn- ->direct-buf ^ByteBuffer [^bytes bs]
  (doto (ByteBuffer/allocateDirect (alength bs))
    (.put bs)
    (.flip)))

(defn open-db
  "Open (or create) a named LMDB database at dir (a java.io.File).
   Returns a map with :env and :db keys."
  [^java.io.File dir ^String db-name]
  (let [env (-> (Env/create)
                (.setMapSize 500485760)
                (.setMaxDbs 20)
                (.open dir (make-array EnvFlags 0)))
        db  (.openDbi env db-name (into-array DbiFlags [DbiFlags/MDB_CREATE]))]
    {:env env :db db}))

(defn put [{:keys [env ^Dbi db]} k v]
  (let [key-buf (->direct-buf (encode k))
        val-buf (->direct-buf (encode v))]
    (with-open [^Txn txn (.txnWrite env)]
      (.put db txn key-buf val-buf (make-array PutFlags 0))
      (.commit txn))))

(defn get-val [{:keys [env ^Dbi db]} k]
  (let [key-buf (->direct-buf (encode k))]
    (with-open [^Txn txn (.txnRead env)]
      (when-let [^ByteBuffer val-buf (.get db txn key-buf)]
        (let [bs (byte-array (.remaining val-buf))]
          (.get val-buf bs)
          (decode bs))))))

(defn- buf->bytes [^ByteBuffer buf]
  (let [bs (byte-array (.remaining buf))]
    (.get buf bs)
    bs))

(defn- encode-prefix ^bytes [v]
  (if (string? v)
    (encode v)
    ;; For collections strip the closing delimiter (] } )) so the bytes are
    ;; a true prefix of any stored key whose first N elements match.
    (let [s   (pr-str v)
          s'  (if (coll? v) (subs s 0 (dec (count s))) s)
          raw (.getBytes ^String s' StandardCharsets/UTF_8)
          buf (byte-array (inc (alength raw)))]
      (aset-byte buf 0 2)
      (System/arraycopy raw 0 buf 1 (alength raw))
      buf)))

(defn prefix-search [{:keys [env ^Dbi db]} prefix]
  (let [prefix-bytes (encode-prefix prefix)
        key-buf      (->direct-buf prefix-bytes)]
    (with-open [^Txn txn (.txnRead env)
                cursor   (.openCursor db txn)]
      (if (.get cursor key-buf GetOp/MDB_SET_RANGE)
        (loop [results []]
          (let [k-bs (buf->bytes (.key cursor))]
            (if (Arrays/equals prefix-bytes 0 (alength prefix-bytes)
                               k-bs         0 (alength prefix-bytes))
              (let [entry [(decode k-bs) (decode (buf->bytes (.val cursor)))]]
                (if (.seek cursor SeekOp/MDB_NEXT)
                  (recur (conj results entry))
                  (conj results entry)))
              results)))
        []))))

(defn delete [{:keys [env ^Dbi db]} k]
  (let [key-buf (->direct-buf (encode k))]
    (with-open [^Txn txn (.txnWrite env)]
      (.delete db txn key-buf)
      (.commit txn))))
