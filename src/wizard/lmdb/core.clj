(ns wizard.lmdb.core
  (:require
   [wizard.codec :as codec :refer [encode encode-val decode]])
  (:import
   (org.lmdbjava Env Dbi DbiFlags EnvFlags GetOp PutFlags SeekOp Txn)
   (java.nio ByteBuffer)
   (java.io File)
   (java.util Arrays)))

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
                (.setMaxDbs 1)
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
        (loop [results (transient [])]
          (let [k-bs (buf->bytes (.key cursor))]
            (if (and (>= (alength k-bs) (alength prefix-bytes))
                     (Arrays/equals prefix-bytes 0 (alength prefix-bytes)
                                    k-bs         0 (alength prefix-bytes)))
              (let [entry [(decode k-bs) (first (decode (buf->bytes (.val cursor))))]]
                (if (.seek cursor SeekOp/MDB_NEXT)
                  (recur (conj! results entry))
                  (persistent! (conj! results entry))))
              (persistent! results))))
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

(defn get-all [{:keys [env db]}]
  (with-open [^Txn txn (.txnRead env)
              ^org.lmdbjava.CursorIterable cursor (.iterate db txn)]
    (let [it (.iterator cursor)
          results (transient [])]
      (while (.hasNext it)
        (let [entry (.next it)
              tupl (.key entry)
              wt (.val entry)]
          (conj! results [(decode (buf->bytes tupl)) (first (decode (buf->bytes wt)))])))
      (persistent! results))))

(comment
  (def db (open-db "/tmp/bench-test" "test"))
  (put db [:asds "cdsdd" 2321] false)
  (prefix-search db [])
  (get-all db))
