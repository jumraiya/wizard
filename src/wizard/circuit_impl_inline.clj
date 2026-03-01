(ns wizard.circuit-impl-inline
  (:require [caudex.circuit :as c]
            [caudex.utils :as utils]
            [caudex.graph :as g]
            [caudex.dbsp :as dbsp]
            [wizard.circuit.state :as c.state]
            [wizard.zset :as z]
            [wizard.circuit-impl-helpers :as helpers]
            [clojure.walk :as w]
            [datascript.built-ins :as d.fns]
            [org.replikativ.persistent-sorted-set :as sset]))


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
   ;; 'inc inc, 'dec dec, 'max max, 'min min,
   ;; 'zero? zero?, 'pos? pos?, 'neg? neg?, 'even? even?, 'odd? odd?, 'compare compare,
   ;; 'rand rand, 'rand-int rand-int,
   ;; 'true? true?, 'false? false?, 'nil? nil?, 'some? some?, 'not not, 'and and-fn, 'or or-fn,
   ;; 'complement complement, 'identical? identical?,
   ;; 'keyword keyword, 'meta meta, 'name name, 'namespace namespace, 'type type
   ,
   ;; 'vector vector, 'list list, 'set set, 'hash-map hash-map, 'array-map array-map,
   ;; 'count count, 'range range, 'not-empty not-empty, 'empty? empty?, 'contains? contains?,
   ;; 'str str, 'subs, subs, 'get get,
   ;; 'pr-str pr-str, 'print-str print-str, 'println-str println-str, 'prn-str prn-str,
   ;; 're-find re-find, 're-matches re-matches, 're-seq re-seq, 're-pattern re-pattern,
   ;; '-differ? -differ?, 'get-else -get-else, 'get-some -get-some, 'missing? -missing?,
   ;; 'clojure.string/blank? clojure.string/blank?, 'clojure.string/includes? str/includes?,
   ;; 'clojure.string/starts-with? str/starts-with?, 'clojure.string/ends-with? str/ends-with?
   ;; 'tuple vector, 'untuple identity
   })


(defmacro tx-data->zset [& body]
  `(apply sset/sorted-set (mapv (fn [[e# a# v# _tx# add?#]]
                                  [e# a# v# add?#])
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
        delta-row-key-sym (gensym)
        join-row-key-sym (gensym)
        new-row-val-sym (gensym)
        lookup-key (mapv #(if (contains? replace-map %)
                            `(nth ~delta-row-sym ~(get replace-map %))
                            :*)
                         (range key-len))]
    `(reduce
      (fn [output# ~delta-row-sym]
        (when (= (symbol "join-17217") '~(:id op))
          (prn
           (c.state/getv ~state-var ~tx-var '~integrated-id)
           ~lookup-key
           (sset/slice (c.state/getv ~state-var ~tx-var '~integrated-id) ~lookup-key ~lookup-key)
           (c.state/slice ~state-var ~tx-var '~integrated-id ~lookup-key)
           (c.state/getv ~state-var ~tx-var '~other-id)))
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


(defmacro gen-zset-for-join [op circuit]
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
      :root `(c.state/put
              ~state-var
              ~tx-var
              '~op-id
              (tx-data->zset (c.state/getv ~state-var ~tx-var :tx-data)))
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
                 (reduce
                  (fn [zset# row#]
                    (z/add-zset-row zset# row#))
                  (z/gen-op-zset ~op)
                  (eduction
                   (filter (fn [~row-sym]
                             (and ~@filter-body)))
                   (map (fn [~row-sym]
                          ~proj))
                   (c.state/getv ~state-var ~tx-var '~input-1)))))
      :map (let [row-sym (gensym)
                 args (into []
                            (map #(if (dbsp/is-idx? %)
                                    `(nth ~row-sym ~(:idx %))
                                    %))
                            (:args op))
                 mapping-fn (get-in fn-syms [(:fn-sym (meta (:mapping-fn op))) (if cljs? :cljs :clj)])
                                        ;(get d.fns/query-fns (:fn-sym (meta (:mapping-fn op))))
                 body (if (some #(when (dbsp/is-idx? %) %) (:args op))
                        `(conj (vec (butlast ~row-sym)) (apply ~mapping-fn ~args) (last ~row-sym))
                        `(vector (apply ~mapping-fn ~args) true))]
             `(c.state/put ~state-var ~tx-var '~op-id
                           (into (z/gen-op-zset ~op)
                                 (map (fn [~row-sym]
                                        ~body))
                                 (c.state/getv ~state-var ~tx-var '~input-1))))
      :neg `(c.state/put ~state-var ~tx-var '~op-id
                         (into (z/gen-op-zset ~op)
                               (map #(conj (vec (butlast %)) (not (last %))))
                               (c.state/getv ~state-var ~tx-var '~input-1)))
      :delay `(c.state/put ~state-var ~tx-var '~op-id
                           (c.state/getv ~state-var '~input-1))
      :integrate `(c.state/add ~state-var ~tx-var '~op-id
                               (into
                                (gen-zset-for-join ~op ~circuit)
                                (c.state/getv ~state-var ~tx-var '~input-1)))
      :join `(c.state/put
              ~state-var ~tx-var '~op-id
              (gen-join-body ~state-var ~tx-var ~input-ops ~op))
      :add `(c.state/put
             ~state-var ~tx-var '~op-id
             (z/add-zsets
              (c.state/getv ~state-var ~tx-var '~input-1)
              (c.state/getv ~state-var ~tx-var '~input-2))))))


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
        cljs? (some? (:ns &env))
        op-fns (reverse
                (conj
                 (mapv (fn [op]
                         `(gen-strata-fn ~circuit ~op ~state-var ~tx-var ~cljs?)) ops)
                 `(fn [~tx-var]
                    (c.state/commit ~state-var ~tx-var)
                    (c.state/getv ~state-var ~tx-var '~final-op-id))))
        xf `(comp ~@op-fns)]
    `(fn [~state-var tx-data#]
       (let [~tx-var (c.state/init-tx ~state-var)
             ~tx-var (c.state/put ~state-var ~tx-var :tx-data tx-data#)]
         (~xf ~tx-var)))))

(defmacro query->circuit [query & [rules]]
  (let [circuit (c/build-circuit (second query) (second rules))]
    `(reify-circuit ~circuit)))

(defmacro edn->circuit [edn]
  (let [circuit (utils/edn->circuit edn)]
    `(reify-circuit ~circuit)))


(comment
  (def query
    '[:find ?o ?accessible
      :where
      [?o :object/desc ?d]
      (not-join [?o]
                [?o :object/desc "player"])
      (or-join [?o ?accessible]
               (and
                [?p :object/loc ?l]
                [?p :object/desc "player"]
                (or-join [?l ?p ?o]
                         [?o :object/loc ?p]
                         [?o :object/loc ?l])
                [(ground :accessible) ?accessible])
               (and
                (not-join [?o]
                          [?p :object/loc ?l]
                          [?p :object/desc "player"]
                          (or-join [?l ?p ?o]
                                   [?o :object/loc ?p]
                                   [?o :object/loc ?l]))
                [(ground :not-accessible) ?accessible]))])

  (def ccirc (c/build-circuit query))
  (def circ (reify-circuit ccirc))
  (def circ
    (query->circuit '[:find ?b
                      :in $ %
                      :where
                      [?a :attr :s]
                      [(inc ?a) ?b]
                      (not-join [?a]
                                [(> ?a 3)])]))
  (circ c-state [[1 :attr :s 123 true]])

  (defn compare-outputs [inline-state simple-ver]
    (doseq [[op v] @(:state inline-state)]
      (let [stream-id (first (get-in @simple-ver [:op-stream-map op :outputs]))
            v2 (last (get-in @simple-ver [:streams stream-id]))]
        (when (not= v v2)
          (prn "output different for" op v v2)))))

  (compare-outputs c-state wizard.circuit-impl/debug-data))
