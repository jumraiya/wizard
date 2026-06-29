(ns wizard.circuit.debug
  (:require
   [caudex.dbsp :as dbsp]
   [caudex.circuit :as c]
   [caudex.impl.circuit :as c.impl]
   #?(:clj [clojure.data.json :as json])
   [caudex.graph :as g]
   [wizard.circuit.state :as state]
   [wizard.lmdb.circuit-state :as l.state]
   [wizard.rocksdb.circuit-state :as r.state]
   [wizard.circuit-impl-inline :as impl-inline]
   [caudex.utils :as utils]
   [wizard.zset :as zs]
   [org.replikativ.persistent-sorted-set :as sset])
  (:import [wizard.circuit.state OpStateRef]))

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
             _ (inline-impl c-state tx)
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
         (doseq [op ops-order]
           (let [ref-output-stream (first (get-in caudex-impl [:op-stream-map (dbsp/-get-id op) :outputs]))
                 ref-data (last (get (:streams caudex-impl) ref-output-stream))
                 op-data (state/getv c-state (dbsp/-get-id op))
                 stream-data (recode-stream-data
                              (if (instance? OpStateRef op-data)
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
         [caudex-impl c-state]))
     [caudex-impl c-state]
     transactions)))

(comment
  (def circuit
    (c/build-circuit
     '[:find ?p ?div ?len
       :where
       [?p :player/div ?div]
       [?p :player/len ?len]]))
  ;(def c-state (l.state/lmdb-state "/tmp/bench-test" circuit))
  (def c-state (state/atom-state circuit))
  (state/get-view c-state)
  ;; (spit "/tmp/circ.edn" (utils/circuit->edn circuit))
  ;; (def circuit (utils/edn->circuit (slurp "/tmp/circ.edn")))
  (let [transactions [[[1 :player/div 4 123 true]
                       [1 :player/len [1 0] 123 true]]
                      [[1 :player/div 4 124 false]
                       [1 :player/len [1 0] 124 false]
                       [1 :player/div 3 124 true]
                       [1 :player/len [3 2] 124 true]]]
        caudex-impl (c.impl/reify-circuit circuit)
        inline-impl (impl-inline/reify-circuit circuit)
        ;; c-state (r.state/rocksdb-state "/tmp/rocksdb" circuit {:debug? true})
        c-state (state/atom-state circuit)]
    (compare-states circuit c-state caudex-impl inline-impl transactions))

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
