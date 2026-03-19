(ns wizard.circuit-impl-inline
  (:require [caudex.circuit :as c]
            [caudex.utils :as utils]
            [caudex.graph :as g]
            [caudex.dbsp :as dbsp]
            [clojure.edn :as edn]
            [wizard.circuit.state :as c.state]
            [wizard.zset :as z]
            [criterium.core :as cr]
            [org.replikativ.persistent-sorted-set :as sset]))


(defn in [coll coll-2]
  (boolean (clojure.set/subset? (set coll-2) (set coll))))

(defn like [s s2]
  (boolean (re-matches (re-pattern (clojure.string/replace s2 "%" ".*")) s)))

(defn not-like [s s2]
  (not (boolean (re-matches (re-pattern (clojure.string/replace s2 "%" ".*")) s))))

(def fn-syms
  {'= {:clj 'clojure.core/= :cljs 'cljs.core/=}
   '== {:clj 'clojure.core/== :cljs 'cljs.core/==}
   'not= {:clj 'clojure.core/not= :cljs 'cljs.core/not=}
   '!= {:clj 'clojure.core/not= :cljs 'cljs.core/not=}
   '< {:clj 'clojure.core/< :cljs 'cljs.core/<}
   '> {:clj 'clojure.core/> :cljs 'cljs.core/>}
   '<= {:clj 'clojure.core/<= :cljs 'cljs.core/<=}
   '>= {:clj 'clojure.core/>= :cljs 'cljs.core/>=}
   '+ {:clj 'clojure.core/+ :cljs 'cljs.core/+}
   '- {:clj 'clojure.core/- :cljs 'cljs.core/-}
   '* {:clj 'clojure.core/* :cljs 'cljs.core/*}
   '/ {:clj 'clojure.core// :cljs 'cljs.core//}
   'quot {:clj 'clojure.core/quot :cljs 'cljs.core/quot}
   'rem {:clj 'clojure.core/rem :cljs 'cljs.core/rem}
   'mod {:clj 'clojure.core/mod :cljs 'cljs.core/mod}
   'ground {:clj 'clojure.core/identity :cljs 'cljs.core/identity}
   'identity {:clj 'clojure.core/identity :cljs 'cljs.core/identity}
   'inc {:clj 'clojure.core/inc :cljs 'cljs.core/inc}
   'dec {:clj 'clojure.core/dec :cljs 'cljs.core/dec}
   'keyword {:clj 'clojure.core/keyword :cljs 'cljs.core/keyword}
   'max {:clj 'clojure.core/max :cljs 'cljs.core/max}
   'min {:clj 'clojure.core/min :cljs 'cljs.core/min}
   'zero? {:clj 'clojure.core/zero? :cljs 'cljs.core/zero?}
   'pos? {:clj 'clojure.core/pos? :cljs 'cljs.core/pos?}
   'neg? {:clj 'clojure.core/neg? :cljs 'cljs.core/neg?}

   'even? {:clj 'clojure.core/even? :cljs 'cljs.core/even?}
   'odd? {:clj 'clojure.core/odd? :cljs 'cljs.core/odd?}
   'compare {:clj 'clojure.core/compare :cljs 'cljs.core/compare}
   'rand {:clj 'clojure.core/rand :cljs 'cljs.core/rand}
   'rand-int {:clj 'clojure.core/rand-int :cljs 'cljs.core/rand-int}
   'true? {:clj 'clojure.core/true? :cljs 'cljs.core/true?}
   'false? {:clj 'clojure.core/false? :cljs 'cljs.core/false?}
   'nil? {:clj 'clojure.core/nil? :cljs 'cljs.core/nil?}
   'some? {:clj 'clojure.core/some? :cljs 'cljs.core/some?}
   'not {:clj 'clojure.core/not :cljs 'cljs.core/not}
   'and {:clj 'clojure.core/and :cljs 'cljs.core/and}
   'tuple {:clj 'clojure.core/vector :cljs 'cljs.core/vector}
   'untuple {:clj 'clojure.core/identity :cljs 'cljs.core/identity}
   're-find {:clj 'clojure.core/re-find :cljs 'cljs.core/re-find}
   're-matches {:clj 'clojure.core/re-matches :cljs 'cljs.core/re-matches}
   're-seq {:clj 'clojure.core/re-seq :cljs 'cljs.core/re-seq}
   're-pattern {:clj 'clojure.core/re-pattern :cljs 'cljs.core/re-pattern}
   'in {:clj 'wizard.circuit-impl-inline/in :cljs 'wizard.circuit-impl-inline/in}
   'like {:clj 'wizard.circuit-impl-inline/like :cljs 'wizard.circuit-impl-inline/like}
   'not-like {:clj 'wizard.circuit-impl-inline/not-like :cljs 'wizard.circuit-impl-inline/not-like}
   ;;'or or-fn
   ;; 'complement complement, 'identical? identical?,
   ;;  'meta meta, 'name name, 'namespace namespace, 'type type
   ,
   ;; 'vector vector, 'list list, 'set set, 'hash-map hash-map, 'array-map array-map,
   ;; 'count count, 'range range, 'not-empty not-empty, 'empty? empty?, 'contains? contains?,
   ;; 'str str, 'subs, subs, 'get get,
   ;; 'pr-str pr-str, 'print-str print-str, 'println-str println-str, 'prn-str prn-str,
   ;; '-differ? -differ?, 'get-else -get-else, 'get-some -get-some, 'missing? -missing?,
   })


(defmacro tx-data->zset [& body]
  `(into (sset/sorted-set-by (z/gen-comparator [0 1 2]))
         (mapv (fn [[e# a# v# _tx# add?#]]
                 (z/->ZSetVecEntry [e# a# v#] add?#)
                                        ;[e# a# v# add?#]
                 )
               ~@body)))


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
        join-row-sym (gensym)
        lookup-key (mapv #(if (contains? replace-map %)
                            `(nth (:tuple ~delta-row-sym) ~(get replace-map %))
                            :*)
                         (range key-len))]
    `(reduce
      (fn [output# ~delta-row-sym]
        (when (= '~(dbsp/-get-id op) '~'join-335489)
         (prn
          "lookup" ~lookup-key
          "int" (wizard.circuit.state/slice ~state-var ~tx-var '~integrated-id ~lookup-key)
          "other" (wizard.circuit.state/getv ~state-var ~tx-var '~other-id)))
        (into output#
              (reduce
               (fn [new-rows# ~join-row-sym]
                 (conj new-rows#
                       ~(if flipped?
                          `(z/join-entry ~delta-row-sym ~join-row-sym)
                          `(z/join-entry ~join-row-sym ~delta-row-sym))))
               []
               (wizard.circuit.state/slice ~state-var ~tx-var '~integrated-id ~lookup-key))))
      (z/gen-op-zset ~op)
      (wizard.circuit.state/getv ~state-var ~tx-var '~other-id))))


(defmacro gen-zset-for-join [state-var op circuit]
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
      `(z/gen-zset-init-body-for-join
        ~(-> op dbsp/-get-output-type dbsp/-to-vector)
        ~(-> other-input :src dbsp/-get-output-type dbsp/-to-vector)))
    `(z/gen-op-zset ~op)))

(defmacro gen-op-body
  [circuit op input-ops state-var tx-var cljs?]
  (let [op-id (dbsp/-get-id op)
        [input-1 input-2] (mapv dbsp/-get-id input-ops)]
    (case (dbsp/-get-op-type op)
      :root `(wizard.circuit.state/put
              ~state-var
              ~tx-var
              '~op-id
              (tx-data->zset (wizard.circuit.state/getv ~state-var ~tx-var :tx-data)))
      :filter (let [filters (:filters op)
                    projections (:projections op)
                    row-sym (gensym)
                    proj  (if (seq projections)
                            `(z/->ZSetVecEntry
                              ~(mapv
                                (fn [{:keys [idx]}]
                                  `(nth (:tuple ~row-sym) ~idx))
                                projections)
                              (:wt ~row-sym))
                            row-sym)
                    filter-body (if (seq filters)
                                  (mapv (fn [[pred & args]]
                                          `(apply
                                            ~(or (:fn-sym (meta pred)) pred)
                                            ~(mapv
                                              (fn [idx|const]
                                                (if (record? idx|const)
                                                  `(get (:tuple ~row-sym) ~(:idx idx|const))
                                        ;`(get ~row-sym ~(:idx idx|const))
                                                  idx|const))
                                              args)))
                                        filters)
                                  [true])]
                `(wizard.circuit.state/put
                  ~state-var
                  ~tx-var
                  '~op-id
                  (reduce
                   (fn [zset# row#]
                     (z/add-row zset# row#)
                     ;(z/add-zset-row zset# row#)
                     )
                   (z/gen-op-zset ~op)
                   (eduction
                    (filter (fn [~row-sym]
                              (and ~@filter-body)))
                    (map (fn [~row-sym]
                           ~proj))
                    (wizard.circuit.state/getv ~state-var ~tx-var '~input-1)))))
      :map (let [row-sym (gensym)
                 args (into []
                            (map #(if (dbsp/is-idx? %)
                                    `(nth (:tuple ~row-sym) ~(:idx %))
                                    ;`(nth ~row-sym ~(:idx %))
                                    %))
                            (:args op))
                 mapping-fn (get-in fn-syms
                                    [(:fn-sym (meta (:mapping-fn op)))
                                     (if cljs? :cljs :clj)]
                                    (:mapping-fn op))
                                        ;(get d.fns/query-fns (:fn-sym (meta (:mapping-fn op))))
                 body (if (some #(when (dbsp/is-idx? %) %) (:args op))
                        `(z/->ZSetVecEntry (conj (:tuple ~row-sym)
                                                 (apply ~mapping-fn ~args))
                                           (:wt ~row-sym))
                        `(z/->ZSetVecEntry [(apply ~mapping-fn ~args)] true))]
             `(wizard.circuit.state/put ~state-var ~tx-var '~op-id
                                        (into (z/gen-op-zset ~op)
                                              (map (fn [~row-sym]
                                                     ~body))
                                              (wizard.circuit.state/getv ~state-var ~tx-var '~input-1))))
      :neg `(wizard.circuit.state/put ~state-var ~tx-var '~op-id
                                      (into (z/gen-op-zset ~op)
                                            (map #(z/->ZSetVecEntry (:tuple %) (not (:wt %))))
                                            (wizard.circuit.state/getv ~state-var ~tx-var '~input-1)))
      :delay `(wizard.circuit.state/put ~state-var ~tx-var '~op-id
                                        (c.state/->OpStateRef '~input-1)
                                        ;(wizard.circuit.state/getv ~state-var '~input-1)
                                        )
      :integrate `(wizard.circuit.state/add ~state-var ~tx-var '~op-id
                                            (reduce z/add-row
                                                    (gen-zset-for-join ~state-var ~op ~circuit)
                                                    (wizard.circuit.state/getv ~state-var ~tx-var '~input-1)))

      :join `(wizard.circuit.state/put
              ~state-var ~tx-var '~op-id
              (gen-join-body ~state-var ~tx-var ~input-ops ~op))
      :add `(wizard.circuit.state/put
             ~state-var ~tx-var '~op-id
             (reduce
              #(z/add-row %1 %2)
              (wizard.circuit.state/getv ~state-var ~tx-var '~input-1)
              (wizard.circuit.state/getv ~state-var ~tx-var '~input-2))))))


(defmacro gen-strata-fn [circuit ops-strata state-var tx-var cljs?]
  (let [x-forms (into []
                      (comp
                       (map (fn get-inputs [op]
                              [op (into []
                                        (map :src)
                                        (sort-by
                                         #(g/attr circuit % :arg)
                                         (g/in-edges circuit op)))]))
                       (map (fn get-body [[op input-ops]]
                              `(gen-op-body ~circuit ~op ~input-ops ~state-var ~tx-var ~cljs?)))
                       (map (fn [body]
                              `(do
                                 ~body))))
                      ops-strata)]
    `(fn [~tx-var]
       (as-> ~tx-var ~tx-var
         ~@x-forms))))

(defmacro reify-circuit
  ([circuit]
   (let [cljs? (some? (:ns &env))]
     `(reify-circuit ~circuit ~cljs?)))
  ([circuit cljs?]
   (let [circuit (if (symbol? circuit)
                   @(resolve circuit)
                   circuit)
         ops (utils/topsort-circuit circuit :stratify? true)
         state-var (gensym)
         tx-var (gensym)
         final-op-id (-> ops last last dbsp/-get-id)
         op-fns (reverse
                 (conj
                  (mapv (fn [op]
                          `(gen-strata-fn ~circuit ~op ~state-var ~tx-var ~cljs?)) ops)
                  `(fn [~tx-var]
                     (wizard.circuit.state/commit ~state-var ~tx-var)
                     (wizard.circuit.state/getv ~state-var ~tx-var '~final-op-id))))
         xf `(comp ~@op-fns)]
     `(fn [~state-var tx-data#]
        (let [~tx-var (wizard.circuit.state/init-tx ~state-var)
              ~tx-var (wizard.circuit.state/put ~state-var ~tx-var :tx-data tx-data#)]
          (into #{}
                (map #(conj (:tuple %) (:wt %)))
                (~xf ~tx-var)))))))

(defmacro query->circuit
  ([query] `(query->circuit ~query nil nil))
  ([query rules] `(query->circuit ~query ~rules nil))
  ([query rules target]
   (let [[query rules] (mapv #(if (instance? clojure.lang.Cons %)
                                (second %) %)
                             [query rules])
         circuit (c/build-circuit query rules)
         cljs? (if (some? target)
                 (= target :cljs)
                 (some? (:ns &env)))]
     `(reify-circuit ~circuit ~cljs?))))

(defmacro edn->circuit
  ([edn] `(edn->circuit ~edn nil))
  ([edn target]
   (let [circuit (utils/edn->circuit edn)
         cljs? (if (some? target)
                 (= target :cljs)
                 (some? (:ns &env)))]
     `(reify-circuit ~circuit ~cljs?))))

(defmacro read-from-file
  ([url] `(read-from-file ~url nil))
  ([url target]
   (let [data (edn/read-string (slurp url))
         circuit (utils/edn->circuit data)
         cljs? (if (some? target)
                 (= target :cljs)
                 (some? (:ns &env)))]
     `(reify-circuit ~circuit ~cljs?))))
