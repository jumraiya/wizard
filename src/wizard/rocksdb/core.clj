(ns wizard.rocksdb.core
  (:require [wizard.codec :as codec]
            [wizard.zset :as zs])
  (:import (org.rocksdb RocksDB BloomFilter Options WriteBatch WriteOptions
                        BlockBasedTableConfig ReadOptions RocksIterator)))

(defn get-db
  ([dir]
   (get-db dir 64))
  ([dir prefix-len]
   (let [table-options (doto (BlockBasedTableConfig.)
                         (.setFilterPolicy (BloomFilter. 10)))
         opts (doto (Options.)
                (.setCreateIfMissing true)
                (.setMaxBackgroundJobs 10)
                (.setTableFormatConfig table-options)
                (.useCappedPrefixExtractor prefix-len))]
     (RocksDB/open opts dir))))

(defn put [^RocksDB db k v]
  (.put db (codec/encode k) (codec/encode v)))

(defn getv [^RocksDB db k]
  (some-> (.get db (codec/encode k))
          codec/decode))

(defn batch-update [db {:keys [puts dels]} & [opts]]
  (with-open [batch (WriteBatch.)]
    (doseq [[k v] puts]
     (.put batch (codec/encode k) (codec/encode v)))
    (doseq [k dels]
      (.delete batch (codec/encode k)))
    (.write db (or opts (WriteOptions.)) batch)))

(defn prefix-slice [db prefix]
  (let [encoded (codec/encode prefix)
        res (transient [])]
    (with-open [opts (doto (ReadOptions.)
                       (.setPrefixSameAsStart true))
                it ^RocksIterator (.newIterator db opts)]
      (.seek it encoded)
      (while (.isValid it)
        (conj! res (zs/->ZSetVecEntry (codec/decode (.key it)) (codec/decode (.value it))))
        (.next it))
      (persistent! res))))

(defn prefix-slice-2 [dir prefix]
  (let [encoded (codec/encode prefix)
        res (transient [])]
    (with-open [db (get-db dir (alength encoded))
                opts (doto (ReadOptions.)
                       (.setPrefixSameAsStart true))
                it ^RocksIterator (.newIterator db opts)]
      (.seek it encoded)
      (while (.isValid it)
        (conj! res (zs/->ZSetVecEntry (codec/decode (.key it)) (codec/decode (.value it))))
        (.next it))
      (persistent! res))))


(defn multi-get [^RocksDB db ks]
  (let [byte-keys (java.util.ArrayList. ^java.util.Collection
                                        (mapv codec/encode ks))
        results (.multiGetAsList db byte-keys)]
    (into {}
          (keep (fn [[k v]] (when v [k (codec/decode v)])))
          (map vector ks results))))

(comment
  (def db (get-db "/tmp/rocksdb"))
  (.close db)
  (put db ['integrate-232 :asd "232" 221.21] [true])
  (put db ['integrate-232 :asd "afasc" 43232] [true])
  (put db ['integrate-232 :vdsdv "bffb" 3432.22] [false])
  (prefix-slice db ['integrate-232 :asd])
  (prefix-slice-2 "/tmp/rocksdb" ['integrate-232 :asd])
  
  (getv db ['integrate-232 :asd "232" 221.21])
  )
