(ns wizard.circuit.state
  (:require [wizard.lmdb.core :as l]
            [wizard.zset :as zs]
            [org.replikativ.persistent-sorted-set :as sset]))

(defprotocol CircuitState
  "Represents an interface to manage durable circuit state, each method operates on a zset, a seqable sorted set"
  (init-tx [this] "Starts a transaction, use when feeding the circuit a new tx-data")
  (getv [this op-id] [this tx op-id])
  (put [this tx op-id zset] "Saves the zset output by an op-id")
  (add [this tx op-id delta] "Adds the delta to the current value of an op but doesn't save it in storage")
  (slice [this tx op-id lookup-key] "Searches the current zset contained in the given op using a lookup key, used for joins")
  (commit [this tx] "Saves the current state into storage")
  (set-debug-mode [this mode])
  (get-op-timings [this]))

(defn- upd [c-state tx]
   (swap! (:state c-state)
          (fn [state]
            (reduce
             (fn [state [k v]]
               (if (= k :deltas)
                 (reduce
                  #(assoc %1 (key %2) (getv c-state tx (key %2))) 
                  state
                  v)
                 (assoc state k (getv c-state tx k))))
             state
             tx))))

(defn- atom-slice [state tx op-id lookup-key]
  (let [entry (zs/->ZSetVecEntry (vec (butlast lookup-key)) (last lookup-key))]
    (sset/slice
     (or (getv state tx op-id)
         (sset/sorted-set))
     entry entry)))

(defrecord OpStateRef [ref-op-id])

(defrecord AtomCircuitState [^clojure.lang.Atom state]
  CircuitState
  (init-tx [_] {})
  (getv [this op-id]
    (clojure.core/get @(:state this) op-id))
  (getv [this tx op-id]
    (or
     (clojure.core/get tx op-id)
     (cond-> (clojure.core/get @(:state this) op-id)
       (contains? (:deltas tx) op-id)
       (zs/add-zset (get-in tx [:deltas op-id])))))
  (slice [this tx op-id lookup-key]
    (let [op-id (if (instance? OpStateRef (get tx op-id))
                  (:ref-op-id (get tx op-id))
                  op-id)]
     (atom-slice this tx op-id lookup-key)))
  (put [_ tx op-id zset] (if (contains? (:deltas tx) op-id)
                           #?(:cljs (js/Error. "Trying to reset a delta state!")
                              :clj (throw (Exception. "Trying to reset a delta state!")))
                           (assoc tx op-id zset)))
  (add [_ tx op-id delta] (assoc-in tx [:deltas op-id] delta))
  (commit [this tx] (upd this tx))
  (set-debug-mode [_ _])
  (get-op-timings [_] nil))


(defn- lmdb-merge
  "Merge a base seq of ZSetVecEntry with a delta, applying cancellation."
  [base delta]
  (let [base-map (into {} (map (fn [r] [(:tuple r) (:wt r)])) base)
        merged   (reduce (fn [m row]
                           (let [k (:tuple row) v (:wt row) e (get m k)]
                             (cond
                               (nil? e) (assoc m k v)
                               (= e v)  m
                               :else    (dissoc m k))))
                         base-map delta)]
    (mapv (fn [[t w]] (zs/->ZSetVecEntry t w)) merged)))


(defn- lmdb-get [ctx tx op-id]
  (let [base (into (sset/sorted-set)
                   (map (fn [[k v]] (zs/->ZSetVecEntry (vec (rest k)) v)))
                   (l/prefix-search ctx [op-id]))]
    (if (contains? (:deltas tx) op-id)
      (lmdb-merge base (get-in tx [:deltas op-id]))
      base)))

(defn- lmdb-overwrite-op-txn [ctx txn op-id data]
  (doseq [[c] (l/prefix-search-txn ctx txn [op-id])]
    (l/delete-txn ctx txn c))
  (doseq [row data]
    (l/put-txn ctx txn (into [op-id] (:tuple row)) (:wt row))))

(defrecord LMDBState [ctx]
  CircuitState
  (init-tx [_] {})
  (getv [_this op-id]
    (into []
          (map (fn [[k v]] (zs/->ZSetVecEntry (vec (rest k)) v)))
          (l/prefix-search ctx [op-id])))
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
                         (map (fn [[k v]] (zs/->ZSetVecEntry (vec (rest k)) v)))
                         (l/prefix-search ctx (into [prefix-op] lk)))
              deltas (when (and (contains? (:deltas tx) prefix-op) (= prefix-op op-id))
                       (filter #(every?
                                 (fn [[idx e]] (= (nth (:tuple %) idx) e))
                                 (map-indexed vector lk))
                               (get-in tx [:deltas prefix-op])))
              v (if (seq deltas)
                  (lmdb-merge base deltas)
                  base)]
          (when (contains? ctx :debug)
            (swap! (:debug ctx) update op-id
                   (fn [{:keys [ms-max ms-min slice-count-max slice-count-min]}]
                     (let [cur-ms (/ (double (- (System/nanoTime) start-t)) 1e6)]
                       {:ms-max (max cur-ms (or ms-max 0))
                        :min-max (min cur-ms (or ms-min 10000))
                        :slice-count-max (max (count base) (or slice-count-max 0))
                        :slice-count-min (min (count base) (or slice-count-min Integer/MAX_VALUE))}))))
          #_(when (= op-id 'delay-647356)
              (prn "lookup" lookup-key
                   "base" base
                   "deltas" deltas
                   "v" v))
          v))))
  (put [_ tx op-id zset] (assoc tx op-id zset))
  (add [_ tx op-id delta] (assoc-in tx [:deltas op-id] delta))
  (commit [_this tx]
    #_(doseq [[op-id data] (filterv #(instance? OpStateRef (val %)) tx)]
        (let [data (into []
                         (map (fn [[k v]] (zs/->ZSetVecEntry (vec (rest k)) v)))
                         (l/prefix-search ctx [(:ref-op-id data)]))]
          (lmdb-overwrite-op-txn ctx op-id data)))
    (let [start-t (when (contains? ctx :debug) (System/nanoTime))]
      (with-open [txn (l/write-txn ctx)]
        (doseq [[op-id delta] (:deltas tx)]
          (doseq [row delta]
            (let [full-key (into [op-id] (:tuple row))
                  cur-wt   (l/get-val-txn ctx txn full-key)]
              (cond
                (nil? cur-wt)           (l/put-txn ctx txn full-key (:wt row))
                (not= cur-wt (:wt row)) (l/delete-txn ctx txn full-key)))))
        (.commit txn))
      (when (contains? ctx :debug)
        (swap! (:debug ctx) update :commit
               (fn [{:keys [ms-max ms-min commit-count-max commit-count-min]}]
                 (let [cur-ms (/ (double (- (System/nanoTime) start-t)) 1e6)
                       commit-count (reduce #(+ %1 (-> %2 val count)) 0 (:deltas tx))]
                   (prn "commit took" cur-ms)
                   {:ms-max (max cur-ms (or ms-max 0))
                    :min-max (min cur-ms (or ms-min 10000))
                    :commit-count-max (max commit-count
                                           (or commit-count-max 0))
                    :commit-count-min (min commit-count
                                           (or commit-count-min Integer/MAX_VALUE))})))))
    #_(doseq [[op-id data] (dissoc tx :deltas)]
        (when (and (not (instance? OpStateRef data))
                   (symbol? op-id))
          (lmdb-overwrite-op-txn ctx op-id data)))
    ;(l/get-val ctx ['root-110316])
    ))

(defn lmdb-state
  ([dir circuit-name]
   (lmdb-state dir circuit-name false))
  ([dir circuit-name debug?]
   (->LMDBState
    (cond->
     (l/open-db dir circuit-name)
      debug? (assoc :debug (atom {}))))))
