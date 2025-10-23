(ns wizard.circuit-impl
  (:require [caudex.utils :as utils]
            [caudex.graph :as g]
            [caudex.dbsp :as dbsp]
            #?(:clj [clojure.data.json :as json])
            [clojure.set :as set]
            [wizard.zset :as z]
            [me.tonsky.persistent-sorted-set :as sset]))
;op types

; root filter map neg delay integrate join add

;(def ^:dynamic *delay-states* (atom {}))

(defonce debug-data (atom nil))


(defn tx-data->zset [tx-data]
  (apply sset/sorted-set (mapv (fn [[e a v _tx add?]]
                                 [e a v add?])
                               tx-data)))


                                        ;#trace
(defn- filter-zset [op]
  (fn [zset]
    (into (z/mk-op-zset op)
          (comp
           (filter (fn [row]
                     (every?
                      #(dbsp/-satisfies? % row)
                      (:filters op))))
           (map (fn [row]
                  (if (seq (:projections op))
                    (conj (mapv #(nth row (:idx %)) (:projections op)) (last row))
                    row))))
          zset)))
;#trace
 (defn- map-zset-row [op row]
   (let [args (into []
                    (map #(if (dbsp/is-idx? %)
                            (nth row (:idx %))
                            %))
                    (:args op))
         indices-used? (some #(when (dbsp/is-idx? %) %) (:args op))
         res (apply (:mapping-fn op) args)]
     (if indices-used?
       (conj (vec (butlast row)) res (last row))
      ;; If no indices were used, simply return the result
       [res true])))
;#trace
(defn- get-join-indices [integrated-op other-op]
  (let [other-input-vars (-> other-op dbsp/-get-output-type dbsp/-to-vector set)]
    (into []
          (comp
           (map-indexed vector)
           (map (fn [[idx var]]
                  (when (and (symbol? var)
                             (contains? other-input-vars var))
                    idx)))
           (filter some?))
          (-> integrated-op dbsp/-get-output-type dbsp/-to-vector))))
;#trace
(defn- init-delay-state-fn [op circuit]
  (if-let [join-output (or
                        (first
                         (into []
                               (comp
                                (map :dest)
                                (filter #(= :join (dbsp/-get-op-type %))))
                               (g/out-edges circuit op)))
                        (first
                         (into []
                               (comp
                                (map :dest)
                                (filter #(= :delay (dbsp/-get-op-type %)))
                                cat
                                (map #(g/out-edges circuit %))
                                cat
                                (map :dest)
                                (filter #(= :join (dbsp/-get-op-type %))))
                               (g/out-edges circuit op))))]
    (let [other-input (first (filter #(not= (:src %) op)
                                     (g/in-edges circuit join-output)))]
      (z/mk-zset-init-fn-for-join
       (-> op dbsp/-get-output-type dbsp/-to-vector)
       (-> other-input :src dbsp/-get-output-type dbsp/-to-vector)))
    #(z/mk-op-zset op)))


#trace
(defn- handle-integrate-op-fn [op circuit input-key]
  (let [init-fn (init-delay-state-fn op circuit)]
    (fn integrate [inputs delay-states]
      (let [old-state (get @delay-states (dbsp/-get-id op) (init-fn))
            new-state (z/add-zsets (get inputs input-key)
                                   old-state)]
        (swap! delay-states
               #(assoc % (dbsp/-get-id op) new-state))
        new-state))))

;#trace
 (defn- handle-delay-op-fn [op input-key]
  (fn [inputs delay-states]
    (let [old-state (get @delay-states (dbsp/-get-id op))]
      (swap! delay-states assoc (dbsp/-get-id op)
             (get inputs input-key))
      old-state)))

#trace
 (defn- handle-join-op [integrated-zset delta-zset lookup-key-fn flipped?]
   (reduce
    (fn [output delta-row]
      (let [lookup-key (lookup-key-fn delta-row)]
        (into output
              (reduce
               (fn [new-rows join-row]
                 (let [delta-row-key (subvec delta-row 0 (-> delta-row count dec))
                       join-row-key (subvec join-row 0 (-> join-row count dec))
                       new-row-val (and (last delta-row) (last join-row))]
                   (conj new-rows
                         (if flipped?
                           (conj (into delta-row-key join-row-key) new-row-val)
                           (conj (into join-row-key delta-row-key) new-row-val)))))
               []
               (if (every? #(= % :*) lookup-key)
                 integrated-zset
                 (when integrated-zset
                  (sset/slice integrated-zset lookup-key lookup-key)))))))
    []
    delta-zset))



(defn process-strata [strata inputs delay-states]
  (reduce
   (fn [outputs [op-id op-fn]]
     (assoc outputs op-id (op-fn inputs delay-states)))
   {}
   strata))

;#trace
 (defn- build-fn [circuit op]
  (let [input-ops (into []
                        (map :src)
                        (sort-by
                         #(g/attr circuit % :arg)
                         (g/in-edges circuit op)))
        input-keys (mapv dbsp/-get-id input-ops)]
    (case (dbsp/-get-op-type op)
      :root (fn [inputs _]
              (tx-data->zset (get inputs 'tx-data)))
      :filter (let [filter-fn (filter-zset op)]
                (fn [inputs _]
                  (filter-fn (get inputs (first input-keys)))))
      :map (fn [inputs _]
             (into (z/mk-op-zset op)
                   (map #(map-zset-row op %))
                   (get inputs (first input-keys))))
      :neg (fn [inputs _]
             (into (z/mk-op-zset op)
                   (map #(conj (vec (butlast %)) (not (last %))))
                   (get inputs (first input-keys))))
      :delay (handle-delay-op-fn op (first input-keys))
      :integrate (handle-integrate-op-fn op circuit (first input-keys))
      :join (let [[lhs rhs] input-keys
                  conds (:join-conds op)
                  [integrated-op other-op flipped? int-op]
                  (if (contains?
                       #{:delay :integrate}
                       (dbsp/-get-op-type (first input-ops)))
                    [lhs rhs false (first input-ops)]
                    [rhs lhs true (second input-ops)])
                  replace-map (into {}
                                    (comp
                                     (map
                                      (fn [[_ l r]]
                                        (if flipped? [r l] [l r])))
                                     (map #(vector (:idx (first %)) (:idx (second %)))))
                                    conds)
                  key-len (->> int-op dbsp/-get-output-type dbsp/-to-vector count inc)
                  lookup-key-fn (fn [delta-row]
                                  (mapv #(if (contains? replace-map %)
                                           (nth delta-row (get replace-map %))
                                           :*)
                                        (range key-len)))]
              (fn [inputs _]
                (handle-join-op (get inputs integrated-op) (get inputs other-op) lookup-key-fn flipped?)))
      :add (fn [inputs _]
             (z/add-zsets
              (get inputs (first input-keys)) (get inputs (second input-keys)))))))

(defn- init-debug-data [circuit]
  (let [id #(str (if (record? %) (dbsp/-get-id %) %))
        label #(str (if (record? %)
                      (str (dbsp/-get-id %)
                           " "
                           (dbsp/-get-output-type %))
                      %))
        nodes (mapv #(hash-map "id" (id %)
                               "label" (label %))
                    (g/nodes circuit))
        edges (mapv #(hash-map "from" (-> % :src id)
                               "to" (-> % :dest id))
                    (g/edges circuit))
        order (utils/topsort-circuit circuit)
        root (utils/get-root-node circuit)
        last-op (last order)
        data (reduce
              (fn [g [idx {:keys [src dest] :as e}]]
                (let [arg-idx (g/attr circuit e :arg)]
                  (-> g
                      (assoc-in [:streams idx] [])
                      (update :op-stream-map
                              (fn [m]
                                (-> m
                                    (update-in [(dbsp/-get-id src) :outputs]
                                               #(conj (or % []) idx))
                                    (update-in [(dbsp/-get-id dest) :inputs]
                                               #(assoc (or % (sorted-map)) arg-idx idx))))))))
              {:t 0
               :streams {-1 [] -2 []}
               :op-stream-map {(dbsp/-get-id root) {:inputs (sorted-map 0 -1) :outputs []}
                               (dbsp/-get-id last-op) {:outputs [-2]}}}
              (eduction
               (map-indexed vector)
               (g/edges circuit)))]
    (assoc data :nodes nodes :edges edges)))

;#trace
(defn- update-debug-data [data outputs]
  (run!
   (fn [[op-id op-output]]
     (let [output-streams (get-in @data [:op-stream-map op-id :outputs])]
       (swap! data update :streams
              #(reduce
               (fn [streams stream-id]
                 (update streams stream-id conj op-output))
               %
               output-streams))))
   outputs)
  (swap! data update :t inc))

;#trace
(defn- dump-debug-data [data]
  (reset! debug-data data)
  #?(:clj (spit "circuit_data.json" (json/write-str data))
     :cljs
     (let [json-str (js/json.stringify (clj->js data) nil 2)
           blob (js/blob. #js [json-str] #js {:type "application/json"})
           url (js/url.createobjecturl blob)
           link (.createelement js/document "a")]
       (set! (.-href link) url)
       (set! (.-download link) "circuit_data.json")
       (.click link)
       (js/url.revokeobjecturl url))))

;#trace
 (defn reify-circuit [circuit & [debug?]]
  (let [strata (utils/topsort-circuit circuit :stratify? true)
        ;; strata-deps (reduce
        ;;              (fn [deps ops]
        ;;                (conj deps
        ;;                      (into #{} (comp (map #(g/in-edges circuit %)) cat (map :src)  (map dbsp/-get-id)) ops)))
        ;;              [#{'tx-data}]
        ;;              (rest strata))
        op-fns (reduce
                #(conj %1 (mapv (fn [op] [(dbsp/-get-id op) (build-fn circuit op)]) %2))
                []
                strata)
        delay-states (atom {})
        debug-data (when debug?
                     (atom (init-debug-data circuit)))]
    (fn [tx-data]
      (loop [outputs {'tx-data tx-data} strata op-fns
                                        ;deps strata-deps
             ]
        (if (seq strata)
          (let [res (process-strata (first strata) outputs delay-states)]
            (recur (into outputs res)
                   (rest strata)))
          (do
            (when debug?
              (dump-debug-data (update-debug-data debug-data outputs)))
            (get outputs (-> op-fns last ffirst))))))))


(comment
  (let [cmp (mk-comparator [0 2])
        data [[1 0 2] [0 5 2] [1 0 1] [0 4 3]]
        sorted (sset/sorted-set-by cmp)
        ;sorted (apply sset/sorted-set-by (into [cmp] data))
        ]
    (into sorted data)
    ;; sorted
    ;; (sort cmp data)
    )
  (def t (sset/sorted-set-by (mk-comparator [0 2]) [1 0 2] [0 5 2] [1 0 1] [0 4 3]))
  (doseq [op (utils/topsort-circuit user/circuit)]
    (let [{:keys [outputs]} (get (:op-stream-map @debug-data) (dbsp/-get-id op))
          stream-data (get-in @debug-data [:streams (first outputs)])
                                        ;_ (prn "stream-data" stream-data)
          to-compare (mapv
                      (fn [zset]
                        (into {}
                              (comp
                               (map
                                #(vector (vec (mapv (fn [v] (if (= v "south") :south v))
                                               (butlast %))) (last %))))
                              zset))
                      stream-data)
          ref-output-stream (first (get-in @caudex.utils/debug-data [:op-stream-map (dbsp/-get-id op) :outputs]))
          ref-data (get (:streams @caudex.utils/debug-data) ref-output-stream)]
      (when (not= to-compare ref-data)
        (prn "mismatch in" (dbsp/-get-id op) to-compare ref-data)))))
