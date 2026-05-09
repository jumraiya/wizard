(ns wizard.rocksdb.circuit-state
  (:require
    [caudex.dbsp :as dbsp]
    [caudex.graph :as g]
    [caudex.utils :as c.utils]
    [clojure.core.reducers :as r]
    [org.replikativ.persistent-sorted-set :as sset]
    [wizard.circuit.state :as st]
    [wizard.rocksdb.core :as rocksdb]
    [wizard.zset :as zs])
  (:import
    (org.rocksdb
      BlockBasedTableConfig
      ColumnFamilyOptions
      LRUCache)
    (wizard.circuit.state
      OpStateRef)))


(defn- get-op-state
  [ctx tx op-id]
  (let [base (when (contains? (:col-handles ctx) op-id)
               (into (sset/sorted-set)
                     (map (fn [[k v]] (zs/->ZSetVecEntry k v)))
                     (rocksdb/get-all ctx op-id)))]
    (if (contains? (:deltas tx) op-id)
      (st/merge-delta base (get-in tx [:deltas op-id]))
      base)))


(defrecord RocksDBState
           [ctx opts]

  st/CircuitState

  (init-tx [_] {})

  (getv
    [_this op-id]
    (into []
          (map (fn [[k v]] (zs/->ZSetVecEntry k v)))
          (rocksdb/get-all ctx op-id)))

  (getv
    [_this tx op-id]
    (let [v (clojure.core/get tx op-id)]
      (if (instance? OpStateRef v)
        (get-op-state ctx tx (:ref-op-id v))
        v)))

  (slice
    [_this op-id lookup-key]
    (let [lk (filterv #(not= :* %) lookup-key)]
      (into (sset/sorted-set)
            (map (fn [[k v]] (zs/->ZSetVecEntry k v)))
            (rocksdb/prefix-slice ctx lk op-id))))

  (slice
    [_this tx op-id lookup-key]
    (let [tx-val (clojure.core/get tx op-id)]
      (if (and (some? tx-val) (not (instance? OpStateRef tx-val)))
        ;; Data is in tx map (filter/join/map/neg ops) - slice in memory
        (let [entry (zs/->ZSetVecEntry (vec (butlast lookup-key)) (last lookup-key))]
          (sset/slice tx-val entry entry))
        (let [prefix-op (if (instance? OpStateRef tx-val)
                          (:ref-op-id tx-val)
                          op-id)
              lk (filterv #(not= :* %) lookup-key)
              base (into (sset/sorted-set)
                         (map (fn [[k v]] (zs/->ZSetVecEntry k v)))
                         (rocksdb/prefix-slice ctx lk prefix-op))
              deltas (when (and (contains? (:deltas tx) prefix-op) (= prefix-op op-id))
                       (into (sset/sorted-set)
                             (sset/slice (get-in tx [:deltas prefix-op])
                                         lookup-key lookup-key)))]
          (if (seq deltas)
            (st/merge-delta base deltas)
            base)))))

  (put [_ tx op-id zset] (assoc tx op-id zset))

  (add [_ tx op-id delta] (assoc-in tx [:deltas op-id] delta))

  (commit
    [_this tx]
    (let [commits (reduce
                   (fn [commits [op-id delta]]
                     (conj
                      commits
                      [op-id
                       (reduce
                        (fn [batch row]
                          (let [cur-wt (rocksdb/getv ctx (:tuple row) op-id)]
                            (if (:initializing? opts)
                              (update batch :puts #(conj % [(:tuple row) (:wt row)]))
                              (cond
                                (nil? cur-wt)
                                (update batch :puts #(conj % [(:tuple row) (:wt row)]))
                                (not= cur-wt (:wt row))
                                (update batch :dels #(conj % (:tuple row)))))))
                        {:puts [] :dels []}
                        delta)]))
                   {}
                   (:deltas tx))
          batch-update (when (:debug? opts)
                         (reduce
                          (fn [b [op-id data]]
                            (if (and (contains? (:col-handles ctx) (name op-id))
                                     (not (contains? (:deltas tx) op-id)))
                              (let [ref-state (when (instance? OpStateRef data)
                                                (into []
                                                      (map (fn [[k v]] (zs/->ZSetVecEntry k v)))
                                                      (rocksdb/get-all ctx (:ref-op-id data))))
                                    new (into {} (map
                                                  #(vector (:tuple %) (:wt %)))
                                              (if ref-state
                                                ref-state
                                                data))
                                    old  (into {} (rocksdb/get-all ctx op-id))]
                                (reduce
                                 (fn [b [k]]
                                   (if (not (contains? new k))
                                     (update-in b [op-id :dels] #(conj (or % []) k))
                                     b))
                                 (assoc-in b [op-id :puts] new)
                                 old))
                              b))
                          {}
                          tx))]
      (rocksdb/batch-update-cols
       ctx (conj commits
                 (let [tx-id (some-> tx :tx-data last (nth 3))]
                   [:default
                    (cond->
                     (reduce
                      (fn [batch row]
                        (let [cur-wt (rocksdb/getv ctx (:tuple row) :default)]
                          (cond
                            (nil? cur-wt) (update batch :puts #(conj %  [(:tuple row) true]))
                            (not= cur-wt (:wt row)) (update batch :dels #(conj % (:tuple row))))))
                      {:puts [] :dels []}
                      (get tx (:output-op opts)))
                      tx-id (update :puts #(conj %  [[:last-processed-tx] tx-id])))])))

      (when batch-update
        (rocksdb/batch-update-cols ctx batch-update))))

  (get-view
    [_this]
    (into []
          (comp (map first)
                (remove #(= [:last-processed-tx] %)))
          (rocksdb/get-all ctx :default)))

  (get-last-processed-tx
    [_]
    (rocksdb/getv ctx [:last-processed-tx] :default))

  (close
    [_]
    (.close (:db ctx))))


(defn rocksdb-state
  [dir circuit & [opts]]
  (let [last-op (last (c.utils/topsort-circuit circuit))
        cols (into []
                   (comp
                     (filter
                       #(and (c.utils/is-op? %)
                             (if (:debug? opts)
                               true
                               (= :integrate (dbsp/-get-op-type %)))))
                     (map #(hash-map :col-name (dbsp/-get-id %))))
                   (g/nodes circuit))
        ;; block-cache (LRUCache. (* 2 1024 1024 1024))
        table-conf (when (:initializing? opts)
                     (doto (BlockBasedTableConfig.)
                       ;; (.setBlockCache block-cache)
                       (.setBlockSize (* 32 1024))
                       (.setCacheIndexAndFilterBlocks true)
                       (.setCacheIndexAndFilterBlocksWithHighPriority true)
                       (.setPinL0FilterAndIndexBlocksInCache true)))
        col-options (when (:initializing? opts)
                      (doto (ColumnFamilyOptions.)
                        (.setWriteBufferSize (* 1024 1024 1024))
                        (.setMaxWriteBufferNumber 4)
                        (.setMinWriteBufferNumberToMerge 2)
                        (.setTableFormatConfig table-conf)))]
    (->RocksDBState (rocksdb/open-db dir
                                     (cond-> {:col-families cols}
                                       col-options
                                       (assoc :col-options col-options)))
                    (assoc opts :output-op (dbsp/-get-id last-op)))))
