(ns wizard.circuit.debug
  (:require
   [caudex.dbsp :as dbsp]
   [caudex.circuit :as c]
   [caudex.impl.circuit :as c.impl]
   #?(:clj [clojure.data.json :as json])
   [caudex.graph :as g]
   [wizard.circuit.state :as state]
   #?(:clj [wizard.lmdb.circuit-state :as l.state])
   #?(:clj [wizard.rocksdb.circuit-state :as r.state])
   [wizard.circuit-impl-inline :as impl-inline]
   [caudex.utils :as utils]
   [wizard.views :as v]
   [wizard.zset :as zs]
   [org.replikativ.persistent-sorted-set :as sset])
  #?(:clj (:import [wizard.circuit.state OpStateRef])))

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

(defn- zv->vec [zv]
  (cond (instance? wizard.zset.ZSetVecEntry zv)
    (when (seq (:tuple zv))
      (conj (:tuple zv) (:wt zv)))
    :else zv))

(defn- update-debug-data [data outputs]
  (run!
   (fn [[op-id op-output]]
     (let [op-output (mapv zv->vec op-output) 
           output-streams (get-in @data [:op-stream-map op-id :outputs])]
       (swap! data update :streams
              #(reduce
                (fn [streams stream-id]
                  (update streams stream-id conj op-output))
                %
                output-streams))))
   outputs)
  (swap! data update :t inc))

(defn- dump-debug-data [debug-data]
  #?(:clj (spit "circuit_data.json" (json/write-str @debug-data))
     :cljs (let [json-str (.stringify js/JSON (clj->js @debug-data) nil 2)
                 blob (js/Blob. #js [json-str] #js {:type "application/json"})
                 url (.createObjectURL js/URL blob)
                 link (.createElement js/document "a")]
             (set! (.-href link) url)
             (set! (.-download link) "circuit_data.json")
             (.click link)
             (.revokeObjectURL js/URL url))))

(defn dump-circuit [circuit]
  (dump-debug-data (atom (init-debug-data circuit))))


(defn compare-states [circuit c-state caudex-impl inline-impl transactions]
  (let [debug-data (atom (init-debug-data circuit))
        ops-order (utils/topsort-circuit circuit)]
    (reduce
     (fn [[caudex-impl c-state] tx]
       (prn "tx" tx)
       (let [caudex-impl (c.impl/step caudex-impl tx)
             res (inline-impl c-state tx)
             recode-stream-data (fn [data]
                                  (into
                                   {}
                                   (comp
                                    (map zv->vec)
                                    (filter some?)
                                    (map #(vector (-> % butlast vec) (last %))))
                                   data))
             _ (update-debug-data debug-data
                                  (into {}
                                        (map #(let [op-id (dbsp/-get-id %)]
                                                [op-id (state/getv c-state op-id)]))
                                        ops-order))]
         (prn "inline res" res "caudex res" (caudex.impl.circuit/get-last-output caudex-impl))
         (prn "view" (state/get-view c-state))
                                        ;(clojure.pprint/pprint (caudex.impl.circuit/get-last-output caudex-impl))
         (doseq [op ops-order]
           (let [ref-output-stream (first (get-in caudex-impl [:op-stream-map (dbsp/-get-id op) :outputs]))
                 ref-data (last (get (:streams caudex-impl) ref-output-stream))
                 op-data (state/getv c-state (dbsp/-get-id op))
                 #_(state/getv c-state (dbsp/-get-id op))
                 stream-data (recode-stream-data
                              op-data
                              #_(if #?(:clj (instance? OpStateRef op-data)
                                       :cljs (instance? state/OpStateRef op-data))
                                  (state/getv c-state (:ref-op-id op-data))
                                  op-data))]
                                        ;(prn (dbsp/-get-id op) stream-data)
             (dump-debug-data debug-data)
             (caudex.utils/circuit->map (assoc caudex-impl :circuit circuit))
                                        ;(throw (Exception. "asd"))
             (when (not= stream-data ref-data)
               (prn (str "mismatch in " (dbsp/-get-id op) " " stream-data " " ref-data))
               #_(throw
                  (ex-info
                   (str "mismatch in " (dbsp/-get-id op) " " stream-data " " ref-data)
                   {:tx tx})))))
         [caudex-impl c-state]
         #_[caudex-impl c-state]))
     [caudex-impl c-state]
     transactions)))

(comment
  (def circuit
    (c/build-circuit
     '[:find ?a ?a-val ?m ?o ?p ?p-name ?p-type ?p-val-long ?p-val-string ?p-val-double
       :in $ %
       :where
       [?a :action/measure ?m]
       [?a :action/offset ?o]
       [?a :action/value ?a-val]
       [?a :action/seq ?seq]
       [?seq :seq/active true]
       (or-join [?seq ?a ?p ?p-type ?p-name ?p-val-long ?p-val-string ?p-val-double]
                (and
                 [?p :param/parent ?seq]
                 [?p :param/name ?p-name]
                 (not-join [?p ?a ?p-name]
                           [?p :param/name ?p-name]
                           [?p :param/parent ?a])
                 (param-val ?p ?p-type ?p-val-long ?p-val-string ?p-val-double))
                (and
                 [?p :param/parent ?a]
                 [?p :param/name ?p-name]
                 (param-val ?p ?p-type ?p-val-long ?p-val-string ?p-val-double))
                (and
                 (not-join [?a ?seq]
                           (or-join [?a ?seq]
                                    [?p :param/parent ?a]
                                    [?p :param/parent ?seq]))
                 [(ground -1) ?p]
                 [(ground "") ?p-name]
                 [(ground :none) ?p-type]
                 [(ground -1) ?p-val-long]
                 [(ground "") ?p-val-string]
                 [(ground 0.0) ?p-val-double]))]
     '[[(param-val ?p ?p-type ?p-val-long ?p-val-string ?p-val-double)
        [?p :param/value ?pv]
        [?p :param/type ?p-type]
        (or-join [?p ?p-type ?pv ?p-val-long ?p-val-string ?p-val-double]
                 (and
                  [(= ?p-type :long)]
                  [?pv :param.value/long ?p-val-long]
                  [(ground "") ?p-val-string]
                  [(ground 0.0) ?p-val-double])
                 (and
                  [(= ?p-type :string)]
                  [?pv :param.value/string ?p-val-string]
                  [(ground 0) ?p-val-long]
                  [(ground 0.0) ?p-val-double])
                 (and
                  [(= ?p-type :double)]
                  [?pv :param.value/double ?p-val-double]
                  [(ground 0) ?p-val-long]
                  [(ground "") ?p-val-string]))]]))
  (def tes
    (v/query->view
     [:find ?a ?a-val ?m ?o ?p ?p-name ?p-type ?p-val-long ?p-val-string ?p-val-double
      :in $ %
      :where
      [?a :action/measure ?m]
      [?a :action/offset ?o]
      [?a :action/value ?a-val]
      [?a :action/seq ?seq]
      [?seq :seq/active true]
      (or-join [?seq ?a ?p ?p-type ?p-name ?p-val-long ?p-val-string ?p-val-double]
               (and
                [?p :param/parent ?seq]
                [?p :param/name ?p-name]
                (not-join [?p ?a ?p-name]
                          [?p :param/name ?p-name]
                          [?p :param/parent ?a])
                (param-val ?p ?p-type ?p-val-long ?p-val-string ?p-val-double))
               (and
                [?p :param/parent ?a]
                [?p :param/name ?p-name]
                (param-val ?p ?p-type ?p-val-long ?p-val-string ?p-val-double))
               (and
                (not-join [?a ?seq]
                          (or-join [?a ?seq]
                                   [?p :param/parent ?a]
                                   [?p :param/parent ?seq]))
                [(ground -1) ?p]
                [(ground "") ?p-name]
                [(ground :none) ?p-type]
                [(ground -1) ?p-val-long]
                [(ground "") ?p-val-string]
                [(ground 0.0) ?p-val-double]))]
     [[(param-val ?p ?p-type ?p-val-long ?p-val-string ?p-val-double)
       [?p :param/value ?pv]
       [?p :param/type ?p-type]
       (or-join [?p ?p-type ?pv ?p-val-long ?p-val-string ?p-val-double]
                (and
                 [(= ?p-type :long)]
                 [?pv :param.value/long ?p-val-long]
                 [(ground "") ?p-val-string]
                 [(ground 0.0) ?p-val-double])
                (and
                 [(= ?p-type :string)]
                 [?pv :param.value/string ?p-val-string]
                 [(ground 0) ?p-val-long]
                 [(ground 0.0) ?p-val-double])
                (and
                 [(= ?p-type :double)]
                 [?pv :param.value/double ?p-val-double]
                 [(ground 0) ?p-val-long]
                 [(ground "") ?p-val-string]))]]))

  (wizard.views/query->view
   [:find ?slot ?ring ?num ?measure ?offset ?div ?len ?active
    ?p-div ?p-len
    :in $ %
    :where
    [?slot :ui.slot/ring ?ring]
    [?slot :ui.slot/measure ?measure]
    [?slot :ui.slot/offset ?offset]
    [?ring :ui.ring/seq ?seq]
    [?ring :ui.ring/num ?num]
    [?seq :seq/active ?active]
    [?seq :seq/div ?div]
    [?seq :seq/len ?len]
    (player-state ?p ?p-div ?p-len)]
   [[(player-state ?p ?p-div ?p-len)
     [?p :player/div ?p-div]
     [?p :player/len ?p-len]]])
                                        ;(def c-state (l.state/lmdb-state "/tmp/bench-test" circuit))
  (def c-state (state/atom-state circuit))
  (state/get-view c-state)
  ;; (spit "/tmp/circ.edn" (utils/circuit->edn circuit))
  ;; (def circuit (utils/edn->circuit (slurp "/tmp/circ.edn")))
  (let [transactions [[[2 :seq/name "test-2" 536870914 true]
                       [2 :seq/div 3 536870914 true]
                       [2 :seq/len [1 0] 536870914 true]
                       [2 :seq/active true 536870914 true]]
                      [[3 :ui.ring/num 2 536870915 true]
                       [3 :ui.ring/seq 2 536870915 true]
                       [1 :player/div 4 536870915 false]
                       [1 :player/div 3 536870915 true]]
                      [[4 :ui.slot/ring 3 536870916 true]
                       [4 :ui.slot/offset 0 536870916 true]
                       [4 :ui.slot/measure 0 536870916 true]
                       [5 :ui.slot/ring 3 536870916 true]
                       [5 :ui.slot/offset 1 536870916 true]
                       [5 :ui.slot/measure 0 536870916 true]
                       [6 :ui.slot/ring 3 536870916 true]
                       [6 :ui.slot/offset 2 536870916 true]
                       [6 :ui.slot/measure 0 536870916 true]]
                      [[7 :action/measure 0 536870915 true]
                       [7 :action/offset 0 536870915 true]
                       [7 :action/seq 2 536870915 true]
                       [7 :action/value "A3" 536870915 true]]
                      [[8 :param/parent 2 536870916 true]
                       [8 :param/type :double 536870916 true]
                       [8 :param/value 9 536870916 true]
                       [8 :param/name "param" 536870916 true]
                       [9 :param.value/double 3.4 536870916 true]]
                      [[10 :param/parent 7 536870917 true]
                       [10 :param/type :double 536870917 true]
                       [10 :param/value 11 536870917 true]
                       [10 :param/name "param" 536870917 true]
                       [11 :param.value/double 2.4 536870917 true]]]
        caudex-impl (c.impl/reify-circuit circuit)
        inline-impl (impl-inline/reify-circuit wizard.circuit.debug/circuit)
        ;; caudex-impl (c.impl/reify-circuit (:circuit tes))
        ;; inline-impl (:circuit-fn tes)
        ;; c-state (state/atom-state (:circuit tes))
        ;; c-state (r.state/rocksdb-state "/tmp/rocksdb" circuit {:debug? true})
        c-state (state/atom-state circuit)
        ]
    (compare-states circuit c-state caudex-impl inline-impl transactions)
    #_(compare-states (:circuit tes) c-state caudex-impl inline-impl transactions))

  (dump-circuit)

  (def op (some #(when (= 'input-17094 (dbsp/-get-id %)) %) (g/nodes circuit)))

  (def zs (into (sset/sorted-set-by (wizard.zset/mk-comparator [1 0 2 3]))
                [[:accessible :player "player" true]
                 [:not-accessible :obj "desc" true]]))
  (sset/slice zs [:* :player :* :*] [:* :player :* :*])
  (def root (.root zs))
  (.len root)
  (.keys root)
  (prn (last (.keys root)))
  (.searchFirst root [:* :player :* :*] (.comparator zs)))
