(ns wizard.circuit-impl-inline
  (:require [caudex.circuit :as c]
            [caudex.utils :as utils]
            [caudex.graph :as g]
            [caudex.dbsp :as dbsp]
            [wizard.circuit.state :as c.state]
            [wizard.zset :as z]
            [datascript.built-ins :as d.fns]
            [me.tonsky.persistent-sorted-set :as sset]))




(defn tx-data->zset [tx-data]
  (apply sset/sorted-set (mapv (fn [[e a v _tx add?]]
                                 [e a v add?])
                               tx-data)))


(defmacro gen-join-body [state-var tx-var input-ops op]
  (let [input-keys (mapv dbsp/-get-id input-ops)
        [lhs rhs] input-keys
        conds (:join-conds op)
        [integrated-id other-id flipped? int-op]
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
        delta-row-sym (gensym)
        delta-row-key-sym (gensym)
        join-row-key-sym (gensym)
        new-row-val-sym (gensym)
        lookup-key (mapv #(if (contains? replace-map %)
                            `(nth ~delta-row-sym ~(get replace-map %))
                            :*)
                         (range key-len))]
    `(reduce
      (fn [output# ~delta-row-sym]
        (into output#
              (reduce
               (fn [new-rows# join-row#]
                 (let [~delta-row-key-sym (subvec ~delta-row-sym 0 (-> ~delta-row-sym count dec))
                       ~join-row-key-sym (subvec join-row# 0 (-> join-row# count dec))
                       ~new-row-val-sym (and (last ~delta-row-sym) (last join-row#))]
                   (conj new-rows#
                         ~(if flipped?
                            `(conj (into ~delta-row-key-sym ~join-row-key-sym) ~new-row-val-sym)
                            `(conj (into ~join-row-key-sym ~delta-row-key-sym) ~new-row-val-sym)))))
               []
               (c.state/slice ~state-var ~tx-var '~integrated-id ~lookup-key))))
      (z/gen-op-zset ~op)
      (c.state/getv ~state-var ~tx-var '~other-id))))



(defn- gen-op-body
  [op input-ops state-var tx-var]
  (let [op-id (dbsp/-get-id op)
        [input-1 input-2] (mapv dbsp/-get-id input-ops)]
    (case (dbsp/-get-op-type op)
                 :root `(c.state/put
                         ~state-var
                         ~tx-var
                         '~op-id
                         (-> ~state-var
                             (c.state/getv ~tx-var :tx-data)
                             (tx-data->zset)))
                 :filter (let [filters (:filters op)
                               projections (:projections op)
                               row-sym (gensym)
                               proj  (if (seq projections)
                                       (conj (mapv #(do `(nth ~row-sym ~(:idx %))) projections) `(last ~row-sym))
                                       row-sym)
                               filter-body (if (seq filters)
                                             (mapv (fn [[pred & args]]
                                                     `(apply
                                                       ~(:fn-sym (meta pred))
                                                       ~(mapv
                                                         (fn [idx|const]
                                                           (if (record? idx|const)
                                                             `(get ~row-sym ~(:idx idx|const))
                                                             idx|const))
                                                         args)))
                                                   filters)
                                             [true])]
                           `(c.state/put
                             ~state-var
                             ~tx-var
                             '~op-id
                             (into (z/gen-op-zset ~op)
                                   (comp
                                    (filter (fn [~row-sym]
                                              (and ~@filter-body)))
                                    (map (fn [~row-sym]
                                           ~proj)))
                                   (c.state/getv ~state-var ~tx-var '~input-1))))
                 :map (let [row-sym (gensym)
                            args (into []
                                       (map #(if (dbsp/is-idx? %)
                                               `(nth ~row-sym ~(:idx %))
                                               %))
                                       (:args op))
                            mapping-fn (get d.fns/query-fns (:fn-sym (meta (:mapping-fn op))))
                            body (if (some #(when (dbsp/is-idx? %) %) (:args op))
                                   `(conj (vec (butlast ~row-sym)) (apply ~mapping-fn ~args) (last ~row-sym))
                                   `(vector (apply ~mapping-fn ~args) true))]
                        `(c.state/put ~state-var ~tx-var '~op-id
                                      (into (z/gen-op-zset ~op)
                                            (map (fn [~row-sym]
                                                   ~body))
                                            (c.state/getv ~state-var ~tx-var '~input-1))))
                 :neg `(c.state/put ~state-var ~tx-var '~op-id
                                    (into (z/mk-op-zset ~op)
                                          (map #(conj (vec (butlast %)) (not (last %))))
                                          (c.state/getv ~state-var ~tx-var '~input-1)))
                 :delay `(c.state/put ~state-var ~tx-var '~op-id
                                      (c.state/getv ~state-var ~tx-var '~input-1))
                 :integrate `(c.state/add ~state-var ~tx-var '~op-id
                                          (c.state/getv ~state-var ~tx-var '~input-1))
                 :join `(c.state/put
                         ~state-var ~tx-var '~op-id
                         (gen-join-body ~state-var ~tx-var ~input-ops ~op))
                 :add `(c.state/put
                        ~state-var ~tx-var '~op-id
                        (z/add-zsets
                         (c.state/getv ~state-var ~tx-var '~input-1)
                         (c.state/getv ~state-var ~tx-var '~input-2))))))


(defn- gen-strata-fn [circuit ops-strata state-var tx-var]
  (let [x-forms (into []
                      (comp
                       (map (fn get-inputs [op]
                              [op (into []
                                        (map :src)
                                        (sort-by
                                         #(g/attr circuit % :arg)
                                         (g/in-edges circuit op)))]))
                       (map (fn get-body [[op input-ops]]
                              (gen-op-body op input-ops state-var tx-var)))
                       (map (fn [body]
                              `(do ~body))))
                      ops-strata)]
    `(fn [~tx-var]
       (as-> ~tx-var ~tx-var
         ~@x-forms))))

(defmacro reify-circuit [circuit]
  (let [circuit (if (symbol? circuit)
                  @(resolve circuit)
                  circuit)
        ops (utils/topsort-circuit circuit :stratify? true)
        state-var (gensym)
        tx-var (gensym)
        final-op-id (-> ops last last dbsp/-get-id)
        op-fns (reverse
                (conj
                 (mapv #(gen-strata-fn circuit % state-var tx-var) ops)
                 `(fn [~tx-var]
                    (c.state/commit ~state-var ~tx-var)
                    (c.state/getv ~state-var ~tx-var '~final-op-id))))
        xf `(comp ~@op-fns)]
    `(fn [~state-var tx-data#]
       (let [~tx-var (c.state/init-tx ~state-var)
             ~tx-var (c.state/put ~state-var ~tx-var :tx-data tx-data#)]
         (~xf ~tx-var)))))

(defmacro query->circuit [query & [rules]]
  (let [circuit (c/build-circuit (second query) rules)]
    `(reify-circuit ~circuit)))



(comment
  (def query '[:find ?b
               :where
               [?a :attr :s]
               [(inc ?a) ?b]
               (not-join [?a]
                         [(> ?a 3)])])

  (def ccirc (c/build-circuit query))
  (def circ
    (query->circuit '[:find ?b
                      :in $ %
                      :where
                      [?a :attr :s]
                      [(inc ?a) ?b]
                      (not-join [?a]
                                [(> ?a 3)])]))
  (circ c-state [[1 :attr :s 123 true]])

  (def circ (query->circuit '[:find ?a ?arg ?o ?det
                              :where
                              [?a :action/arg ?arg]
                              [?a :action/type ?action-type]
                              [?a :action/type :inspect]
                              (not-join [?a]
                                        [?a :action/inspect-processed? true])
                              (or-join [?o ?arg ?det]
                                       (and
                                        [(not= ?arg "player")]
                                        [?o :object/description ?arg]
                                        [?o :object/detailed-description ?det]
                                        [?p :object/description "player"]
                                        [?p :object/location ?l]
                                        (or-join [?l ?p ?o]
                                                 [?o :object/location ?p]
                                                 [?o :object/location ?l]))
                                       (and
                                        (or-join [?arg]
                                                 [(= ?arg "player")]
                                                 (not-join [?arg]
                                                           [_ :object/description ?arg])
                                                 (and
                                                  [?o :object/description ?arg]
                                                  [(not= ?arg "player")]
                                                  (not-join [?o]
                                                            [?p :object/description "player"]
                                                            [?p :object/location ?l]
                                                            (or-join [?l ?p ?o]
                                                                     [?o :object/location ?p]
                                                                     [?o :object/location ?l]))))
                                        [(ground :not-found) ?o]
                                        [(ground "no-description") ?det]))]))
  (def c-state (c.state/->AtomCircuitState (atom {})))
  (circ c-state [[:obj :object/description "desc" 123 true]
                 [:obj :object/detailed-description "detailed desc" 123 true]
                 [:obj :object/location :loc 123 true]
                 [:player :object/description "player" 123 true]])

  (circ c-state [[:player :object/location :loc 123 true]])
  (circ c-state [[:action-1 :action/type :move 124 true]
                 [:action-1 :action/arg "south" 124 true]])
  (circ c-state [[:action-2 :action/type :inspect 124 true]
                 [:action-2 :action/arg "desc" 124 true]])
  (circ c-state [[:action-2 :action/inspect-processed? true 124 true]])
  (circ c-state [[:player :object/location :loc 123 false]
                 [:player :object/location :loc-2 123 true]])

  (circ c-state [[:action-3 :action/type :inspect 124 true]
                 [:action-3 :action/arg "desc" 124 true]])
  )
