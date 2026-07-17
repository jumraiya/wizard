(ns wizard.circuit.state
  (:require
    [caudex.dbsp :as dbsp]
    [caudex.utils :as utils]
    [org.replikativ.persistent-sorted-set :as sset]
    [wizard.zset :as zs]))


(defprotocol CircuitState
  "Represents an interface to manage durable circuit state, each method operates on a zset, a seqable sorted set"

  (init-tx
    [this]
    "Starts a transaction, use when feeding the circuit a new tx-data")

  (getv
    [this op-id]
    [this tx op-id])

  (put
    [this tx op-id zset]
    "Saves the zset output by an op-id")

  (add
    [this tx op-id delta]
    "Adds the delta to the current value of an op but doesn't save it in storage")

  (slice
    [this op-id lookup-key]
    [this tx op-id lookup-key]
    "Searches the current zset contained in the given op using a lookup key, used for joins")

  (commit
    [this tx]
    "Saves the current state into storage")

  (get-view [this])

  (get-last-processed-tx [this])

  (start-checkpoint! [this])

  (stop-checkpoint! [this])

  (cleanup-checkpoint! [this])

  (get-tx-since-checkpoint [this])
  
  (rollback-to-checkpoint! [this])

  (close [this]))

(defn- upd
  [c-state tx]
  (swap! (:state c-state)
         (fn [state]
           (reduce
             (fn [state [k v]]
               (case k
                 :deltas
                 (reduce
                   #(assoc %1 (key %2) (getv c-state tx (key %2)))
                   state
                   v)
                 (assoc state k (getv c-state tx k))))
             state
             tx))))

(defn- atom-slice
  [state tx op-id lookup-key]
  (let [entry (zs/->ZSetVecEntry (vec (butlast lookup-key)) (last lookup-key))]
    (into (sset/sorted-set)
          (sset/slice
           (or (getv state tx op-id)
               (sset/sorted-set))
           entry entry))))


(defrecord OpStateRef
  [ref-op-id])


(defn merge-delta
  "Merge a base seq of ZSetVecEntry with a delta, applying cancellation."
  [base delta]
  (reduce zs/add-row
          base
          delta))

(defn getv* [state tx op-id]
  (let [v (clojure.core/get tx op-id)]
    (if (instance? OpStateRef v)
      (clojure.core/get @state (:ref-op-id v))
      (if (contains? (:deltas tx) op-id)
        (zs/add-zset (clojure.core/get @state op-id) (get-in tx [:deltas op-id]))
        v))))

(defn- slice* [this tx op-id lookup-key]
  (let [op-id' (if (instance? OpStateRef (get tx op-id))
                 (:ref-op-id (get tx op-id))
                 op-id)
        base (atom-slice this tx op-id lookup-key)
        ;; deltas (when (and (contains? (:deltas tx) op-id') (= op-id op-id'))
        ;;          (into (sset/sorted-set)
        ;;                (sset/slice (get-in tx [:deltas op-id'])
        ;;                            lookup-key lookup-key)))
        ]
    base
    #_(if (seq deltas)
        (merge-delta base deltas)
        base)))

(defrecord AtomCircuitState
           [^clojure.lang.Atom state]

  CircuitState

  (init-tx [_] {})

  (getv
    [this op-id]
    (clojure.core/get @(:state this) op-id))

  (getv
    [this tx op-id]
    (getv* state tx op-id))

  (slice
    [this tx op-id lookup-key]
    (slice* this tx op-id lookup-key))

  (put
    [_ tx op-id zset]
    (if (contains? (:deltas tx) op-id)
      #?(:cljs (js/Error. "Trying to reset a delta state!")
         :clj (throw (Exception. "Trying to reset a delta state!")))
      (assoc tx op-id zset)))

  (add [_ tx op-id delta] (assoc-in tx [:deltas op-id] delta))

  (commit
    [this tx]
    (upd this tx)
    (swap! state
           (fn [state]
             (cond-> state
               true
               (update :view
                       (fn [view]
                         (persistent!
                          (reduce
                           #(if (contains? %1 (:tuple %2))
                              (if (false? (:wt %2))
                                (disj! %1 (:tuple %2))
                                %1)
                              (if (true? (:wt %2))
                               (conj! %1 (:tuple %2))
                               %1))
                           (transient view)
                           (get tx (:output-op state))))))
               true
               (assoc :last-processed-tx (some-> tx :tx-data last (nth 3)))
               (:checkpoint-enabled? state)
               (update :tx-since-checkpoint
                       conj (assoc-in tx [:deltas :view]
                                      (-> state :output-op tx))))))
    nil)

  (get-view
    [_this]
    (-> @state :view))

  (get-last-processed-tx
    [_]
    (:last-processed-tx @state))

  (start-checkpoint! [_]
    (when (:checkpoint-enabled? @state)
      #?(:cljs (throw (js/Error. "Checkpoint aleady enabled!"))
         :clj (throw (Exception. "Checkpoint already enabled!"))))
    (swap! state
           (fn [s]
             (assoc s
                    :checkpoint-enabled? true
                    :tx-since-checkpoint []))))

  (stop-checkpoint! [_]
    (swap! state dissoc :checkpoint-enabled?))

  (cleanup-checkpoint! [_]
    (swap! state dissoc :tx-since-checkpoint))

  (get-tx-since-checkpoint [_]
    (:tx-since-checkpoint @state))

  (rollback-to-checkpoint! [_]
    (swap! state
           (fn [s]
             (reduce
              (fn [s tx]
                (reduce
                 (fn [s [op-id delta]]
                   (if (= op-id :view)
                     (update s op-id
                             #(into (sset/sorted-set)
                                    (map :tuple)
                                    (zs/add-zset
                                     (into (sset/sorted-set)
                                           (map (fn [e] (zs/mk-zset-entry e true)))
                                           %)
                                     (mapv (fn [row] (update row :wt not)) delta))))
                     (update s op-id
                             #(zs/add-zset
                               %
                               (mapv (fn [row] (update row :wt not)) delta)))))
                 s
                 (-> tx :deltas reverse)))
              s
              (:tx-since-checkpoint s)))))

  (close [_]))


(defn atom-state
  [circuit]
  (let [last-op (last (utils/topsort-circuit circuit))]
    (->AtomCircuitState (atom {:output-op (dbsp/-get-id last-op) :view (sset/sorted-set)}))))
