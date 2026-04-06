(ns wizard.lmdb.circuit-state
  (:require [wizard.lmdb.core :as l]
            [wizard.zset :as zs]
            [wizard.circuit.state :as st]
            [caudex.graph :as g]
            [caudex.utils :as c.utils]
            [caudex.dbsp :as dbsp]
            [org.replikativ.persistent-sorted-set :as sset])
  (:import [wizard.circuit.state OpStateRef]
           [java.nio.file Files Path Paths]
           [java.nio.file.attribute PosixFilePermissions]))

(defn- lmdb-get [ctx tx op-id]
  (let [base (into (sset/sorted-set)
                   (map (fn [[k v]] (zs/->ZSetVecEntry k v)))
                   (l/get-all (get ctx op-id)))]
    (if (contains? (:deltas tx) op-id)
      (st/merge-delta base (get-in tx [:deltas op-id]))
      base)))

(defn- lmdb-overwrite-op-txn [ctx txn op-id data]
  (doseq [[c] (l/get-all (get ctx op-id))]
    (l/delete-txn (get ctx op-id) txn c))
  (doseq [row data]
    (l/put-txn (get ctx op-id) txn (:tuple row) (:wt row))))

(defrecord LMDBState [ctx opts]
  st/CircuitState
  (init-tx [_] {})
  (getv [_this op-id]
    (into []
          (map (fn [[k v]] (zs/->ZSetVecEntry k v)))
          (l/get-all (get ctx op-id))))
  (getv [_this tx op-id]
    (let [v (clojure.core/get tx op-id)]
      (if (instance? OpStateRef v)
        (lmdb-get ctx tx (:ref-op-id v))
        v)))
  (slice [_this tx op-id lookup-key]
    (let [tx-val (clojure.core/get tx op-id)]
      (if (and (some? tx-val) (not (instance? OpStateRef tx-val)))
        ;; Data is in tx map (filter/join/map/neg ops) - slice in memory
        (let [entry (zs/->ZSetVecEntry (vec (butlast lookup-key)) (last lookup-key))]
          (sset/slice tx-val entry entry))
        ;; Data is in LMDB (integrate ops) or OpStateRef (delay)
        (let [prefix-op (if (instance? OpStateRef tx-val)
                          (:ref-op-id tx-val)
                          op-id)
              lk (filterv #(not= :* %) lookup-key)
              start-t (when (contains? ctx :debug)
                        (System/nanoTime))
              base (into []
                         (map (fn [[k v]] (zs/->ZSetVecEntry k v)))
                         (l/prefix-search (get ctx prefix-op) lk))
              deltas (when (and (contains? (:deltas tx) prefix-op) (= prefix-op op-id))
                       (filter #(every?
                                 (fn [[idx e]] (= (nth (:tuple %) idx) e))
                                 (map-indexed vector lk))
                               (get-in tx [:deltas prefix-op])))
              v (if (seq deltas)
                  (st/merge-delta base deltas)
                  base)]
          (when (contains? ctx :debug)
            (swap! (:debug ctx) update op-id
                   (fn [{:keys [ms-max ms-min slice-count-max slice-count-min]}]
                     (let [cur-ms (/ (double (- (System/nanoTime) start-t)) 1e6)]
                       {:ms-max (max cur-ms (or ms-max 0))
                        :min-max (min cur-ms (or ms-min 10000))
                        :slice-count-max (max (count base) (or slice-count-max 0))
                        :slice-count-min (min (count base) (or slice-count-min Integer/MAX_VALUE))}))))
          v))))
  (put [_ tx op-id zset] (assoc tx op-id zset))
  (add [_ tx op-id delta] (assoc-in tx [:deltas op-id] delta))
  (commit [_this tx]
    (when (:debug opts)
      (doseq [[op-id data] (filterv #(instance? OpStateRef (val %)) tx)]
        (let [data (into []
                         (map (fn [[k v]] (zs/->ZSetVecEntry (vec (rest k)) v)))
                         (l/prefix-search ctx [(:ref-op-id data)]))]
          (lmdb-overwrite-op-txn ctx op-id data))))
    (let [start-t (when (:debug opts) (System/nanoTime))
          commits (mapv
                   (fn [[op-id delta]]
                     (future
                       (let [op-ctx (get ctx op-id)]
                         (with-open [txn (l/write-txn op-ctx)]
                           (doseq [row delta]
                             (if (:initializing? opts)
                               (l/put-txn op-ctx txn (:tuple row) (:wt row))
                               (let [cur-wt (l/get-val-txn op-ctx txn (:tuple row))]
                                 (cond
                                   (nil? cur-wt) (l/put-txn op-ctx txn (:tuple row) (:wt row))
                                   (not= cur-wt (:wt row)) (l/delete-txn op-ctx txn (:tuple row))))))
                           (.commit txn)))))
                   (:deltas tx))]
      (run! deref commits)
      (when (contains? ctx :debug)
        (swap! (:debug ctx) update :commit
               (fn [{:keys [ms-max ms-min commit-count-max commit-count-min]}]
                 (let [cur-ms (/ (double (- (System/nanoTime) start-t)) 1e6)
                       commit-count (reduce #(+ %1 (-> %2 val count)) 0 (:deltas tx))]
                   {:ms-max (max cur-ms (or ms-max 0))
                    :min-max (min cur-ms (or ms-min 10000))
                    :commit-count-max (max commit-count
                                           (or commit-count-max 0))
                    :commit-count-min (min commit-count
                                           (or commit-count-min Integer/MAX_VALUE))})))))
    (when (:debug opts)
     (doseq [[op-id data] (dissoc tx :deltas)]
       (when (and (not (instance? OpStateRef data))
                  (symbol? op-id))
         (lmdb-overwrite-op-txn ctx op-id data))))))

(defn lmdb-state
  [dir circuit & [opts]]
  (let [dbs (into {}
                  (comp
                   (filter
                    #(and (c.utils/is-op? %)
                          (= :integrate (dbsp/-get-op-type %))))
                   (map #(let [id (dbsp/-get-id %)
                               path (Paths/get dir (into-array String [(name id)]))
                               abs-path (.toString (.toAbsolutePath path))]
                           (Files/createDirectories
                            path
                            (into-array
                             [(PosixFilePermissions/asFileAttribute (PosixFilePermissions/fromString "rwxr-x---"))]))
                           [id (l/open-db abs-path (name id))])))
                  (g/nodes circuit))]
    (->LMDBState dbs opts)))

(comment
  (def circ (caudex.circuit/build-circuit '[:find ?a ?b
                                            :where
                                            [?a :attr-1 ?b]]))
  (let [dir "/tmp/lmdb-test"]
    (into [] (comp
              (filter
               #(and (c.utils/is-op? %)
                     (= :integrate (dbsp/-get-op-type %))))
              (map #(let [id (name (dbsp/-get-id %))
                          path (Paths/get dir (into-array String [id]))
                          abs-path (.toString (.toAbsolutePath path))]
                      [id `(l/open-db abs-path id)])))
          (g/nodes circ)))
  (c.utils/is-op? (first (g/nodes circ)))
  (def st (lmdb-state "/tmp/lmdb-test" circ)))
