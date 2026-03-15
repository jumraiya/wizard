(ns wizard.circuit.state
  (:require [wizard.lmdb :as l]
            [wizard.zset :as zs]
            [org.replikativ.persistent-sorted-set :as sset]))

(defprotocol CircuitState
  "Represents an interface to manage durable circuit state, each method operates on a zset, a seqable sorted set"
  (init-tx [this] "Starts a transaction, use when feeding the circuit a new tx-data")
  (getv [this op-id] [this tx op-id])
  (put [this tx op-id zset] "Saves the zset output by an op-id")
  (add [this tx op-id delta] "Adds the delta to the current value of an op but doesn't save it in storage")
  (slice [this tx op-id lookup-key] "Searches the current zset contained in the given op using a lookup key, used for joins")
  (commit [this tx] "Saves the current state into storage"))

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
    (atom-slice this tx op-id lookup-key))
  (put [_ tx op-id zset] (if (contains? (:deltas tx) op-id)
                           #?(:cljs (js/Error. "Trying to reset a delta state!")
                              :clj (throw (Exception. "Trying to reset a delta state!")))
                           (assoc tx op-id zset)))
  (add [_ tx op-id delta] (assoc-in tx [:deltas op-id] delta))
  (commit [this tx] (upd this tx)))


(defrecord LMDBState [ctx]
  CircuitState
  (init-tx [_] {})
  (getv [_this op-id]
    (l/prefix-search ctx [op-id]))
  (getv [_this tx op-id]
    (or
     (clojure.core/get tx op-id)
     (cond-> (l/prefix-search ctx [op-id])
       (contains? (:deltas tx) op-id)
       (zs/add-zsets (get-in tx [:deltas op-id])))))
  (slice [_this tx op-id lookup-key]
    (let [lookup-key (filterv #(not= :* %) lookup-key)]
      (reduce
       zs/add-zset-row
       (into
        (sset/sorted-set)
        (l/prefix-search ctx [op-id lookup-key]))
       (filter #(every? (fn [[idx e]]
                          (= (nth lookup-key idx) e))
                        (map-indexed vector %))
               (-> tx :deltas op-id)))))
  (put [_ tx op-id zset] (if (contains? (:deltas tx) op-id)
                           #?(:cljs (js/Error. "Trying to reset a delta state!")
                              :clj (throw (Exception. "Trying to reset a delta state!")))
                           (assoc tx op-id zset)))
  (add [_ tx op-id delta] (assoc-in tx [:deltas op-id] delta))
  (commit [this tx] (upd this tx)))

(defn lmdb-state [dir circuit-name]
  (->LMDBState
   (l/open-db dir circuit-name)))
