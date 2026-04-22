(ns wizard.rocksdb.core
  (:require [wizard.codec :as codec]
            [wizard.zset :as zs]
            [clojure.java.io :as io])
  (:import (org.rocksdb RocksDB BloomFilter Options WriteBatch WriteOptions
                        ColumnFamilyOptions ColumnFamilyDescriptor DBOptions
                        BlockBasedTableConfig ReadOptions RocksIterator Slice)
           (java.util Arrays ArrayList)))

(defn open-db
  ([dir]
   (open-db dir {}))
  ([dir {:keys [col-families col-options]}]
   (let [;; table-options (doto (BlockBasedTableConfig.)
         ;;                 (.setFilterPolicy (BloomFilter. 10)))
         opts (doto (Options.)
                (.setCreateIfMissing true)
                (.setCreateMissingColumnFamilies true)
                (.setMaxBackgroundJobs 10)
                ;; (.setDbWriteBufferSize (* 1024 1024 1024 10))
                                        ;(.setTableFormatConfig table-options)
                                        ;(.useCappedPrefixExtractor prefix-len)
                )

         [col-families col-descs] (when col-families
                                    [(into [{:col-name :default}]
                                           col-families)
                                     (into
                                      [(ColumnFamilyDescriptor.
                                        RocksDB/DEFAULT_COLUMN_FAMILY
                                        (or col-options (ColumnFamilyOptions.)))]
                                      (mapv #(ColumnFamilyDescriptor.
                                              (.getBytes (name (:col-name %)))
                                              (ColumnFamilyOptions.))
                                            col-families))])
         col-handles (ArrayList.)
         db (cond
              col-descs (RocksDB/open (DBOptions. opts) dir col-descs col-handles)
              :else (RocksDB/open opts dir))]
     {:db db
      :col-handles (into {}
                         (map (fn [[idx {:keys [col-name]}]]
                                [(name col-name) (.get col-handles idx)]))
                         (map-indexed vector col-families))})))

(defn put [{:keys [db col-handles]} k v & [col-name]]
  (if col-name
    (.put db (get col-handles (name col-name)) (codec/encode k) (codec/encode [v]))
    (.put db (codec/encode k) (codec/encode [v]))))

(defn getv [{:keys [db col-handles]} k & [col-name]]
  (first (some-> (if col-name
                   (.get db (get col-handles (name col-name)) (codec/encode k))
                   (.get db (codec/encode k)))
                 codec/decode)))

(defn batch-update [ctx {:keys [puts dels]} & [opts]]
  (with-open [batch (WriteBatch.)]
    (doseq [[k v] puts]
      (.put batch (codec/encode k) [(codec/encode-val v)]))
    (doseq [k dels]
      (.delete batch (codec/encode k)))
    (.write (:db ctx) (or opts (WriteOptions.)) batch)))


(defn batch-update-cols [ctx col-updates & [opts]]
  (with-open [batch (WriteBatch.)]
    (doseq [[col {:keys [puts dels]}] col-updates]
      (doseq [[k v] puts]
        (.put batch (-> ctx :col-handles (get (name col)))
              (codec/encode k) (codec/encode [v])))
      (doseq [k dels]
        (.delete batch (-> ctx :col-handles (get (name col)))
                 (codec/encode k))))
    (.write (:db ctx) (or opts (doto (WriteOptions.) (.disableWAL))) batch)))

(defn- get-prefix-upper-bound [prefix-bytes]
  (loop [i (-> prefix-bytes alength dec)]
    (if (not= 0xFF (aget prefix-bytes i))
      (let [bound (Arrays/copyOfRange prefix-bytes 0 (inc i))]
        (aset bound i (unchecked-byte (inc (aget bound i))))
        (Slice. bound))
      (when (> i 0)
        (recur (dec i))))))

(defn prefix-slice [{:keys [db col-handles]} prefix & [col-name]]
  (let [prefix-bytes (codec/encode prefix)
        res (transient [])]
    (with-open [upper-bound (get-prefix-upper-bound prefix-bytes)
                read-opts (doto (ReadOptions.)
                            (.setIterateUpperBound upper-bound))
                it ^RocksIterator
                (if col-name
                  (.newIterator db (get col-handles (name col-name)) read-opts)
                  (.newIterator db read-opts))]
      (.seek it prefix-bytes)
      (while (.isValid it)
        (conj! res
               [(codec/decode (.key it))
                (first (codec/decode (.value it)))])
        (.next it)))
    (persistent! res)))

(defn get-all [{:keys [db col-handles]} & [col-name]]
  (let [res (transient [])]
    (with-open [read-opts (ReadOptions.)
                it ^RocksIterator (if col-name
                                    (.newIterator db (get col-handles (name col-name)) read-opts)
                                    (.newIterator db read-opts))]
      (.seek it (byte-array [0x00]))
      (while (.isValid it)
        (conj! res [(codec/decode (.key it))
                    (first (codec/decode (.value it)))])
        (.next it)))
    (persistent! res)))

;; (defn prefix-slice [db prefix]
;;   (let [encoded (codec/encode prefix)
;;         res (transient [])]
;;     (with-open [opts (doto (ReadOptions.)
;;                        (.setPrefixSameAsStart true))
;;                 it ^RocksIterator (.newIterator db opts)]
;;       (.seek it encoded)
;;       (while (.isValid it)
;;         (conj! res (zs/->ZSetVecEntry (codec/decode (.key it)) (codec/decode (.value it))))
;;         (.next it))
;;       (persistent! res))))

;; (defn prefix-slice-2 [dir prefix]
;;   (let [encoded (codec/encode prefix)
;;         res (transient [])]
;;     (with-open [db (get-db dir (alength encoded))
;;                 opts (doto (ReadOptions.)
;;                        (.setPrefixSameAsStart true))
;;                 it ^RocksIterator (.newIterator db opts)]
;;       (.seek it encoded)
;;       (while (.isValid it)
;;         (conj! res (zs/->ZSetVecEntry (codec/decode (.key it)) (codec/decode (.value it))))
;;         (.next it))
;;       (persistent! res))))


(defn create-column-family [db col-name]
  (.createColumnFamily
   db
   (ColumnFamilyDescriptor. (.getBytes (if (or (keyword? col-name)
                                               (symbol? col-name))
                                         (name col-name)
                                         col-name))
                            (ColumnFamilyOptions.))))

(defn multi-get [^RocksDB db ks]
  (let [byte-keys (java.util.ArrayList. ^java.util.Collection
                   (mapv codec/encode ks))
        results (.multiGetAsList db byte-keys)]
    (into {}
          (keep (fn [[k v]] (when v [k (first (codec/decode v))])))
          (map vector ks results))))

(comment
  (def ctx (open-db "/tmp/rocksdb"
                    {:col-families
                     [{:col-name 'integrate-232}
                      {:col-name 'integrate-433}]}))
  (.close (:db ctx))
  (put (:db ctx) ['integrate-232 :asd "232" 221.21] [true])
  ;;fail
  (batch-update-cols ctx
                     {'integrate-232 {:puts [[[:asd "232" 221.21] [true]]]}})

  (prefix-slice ctx [:asd] 'integrate-232)

  (prefix-slice ctx ['integrate-232])

  (put db ['integrate-232 :asd "afasc" 43232] [true])
  (put db ['integrate-232 :vdsdv "bffb" 3432.22] [false])

  (getv (:db ctx) ['integrate-232 :asd "232" 221.21])
  (batch-update db {:puts [[['integrate-232 :asd "232" 221.21] [true]]
                           [['integrate-232 :asd "afasc" 43232] [true]]
                           [['integrate-232 :vdsdv "bffb" 3432.22] [false]]]})

  (multi-get db [['integrate-232 :asd "232" 221.21]
                 ['integrate-232 :vdsdv "bffb" 3432.22]])
  (prefix-slice (:db ctx) nil)
  (prefix-slice-2 "/tmp/rocksdb" ['integrate-232 :asd])

  (create-column-family db 'integrate-2322)
  (get-all ctx 'integrate-232)
  (getv db ['integrate-232 :asd "232" 221.21]))
