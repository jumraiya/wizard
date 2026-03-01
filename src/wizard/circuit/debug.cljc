(ns wizard.circuit.debug
  (:require
   [caudex.dbsp :as dbsp]
   [caudex.circuit :as c]
   [caudex.impl.circuit :as c.impl]
   #?(:clj [clojure.data.json :as json])
   [caudex.graph :as g]
   [wizard.circuit.state :as state]
   [wizard.circuit-impl-inline :as impl-inline]
   [caudex.utils :as utils]
   [wizard.zset :as zs]
   [org.replikativ.persistent-sorted-set :as sset]))

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

#trace
 (defn compare-states [circuit caudex-impl inline-impl transactions]
   (let [inline-state (atom {})
         c-state (state/->AtomCircuitState inline-state)
         debug-data (atom (init-debug-data circuit))
         ops-order (utils/topsort-circuit circuit)]
     (reduce
      (fn [[caudex-impl c-state] tx]
        (let [caudex-impl (c.impl/step caudex-impl tx)
              _ (inline-impl c-state tx)
              _ (update-debug-data debug-data @inline-state)]
          (doseq [op ops-order]
            (let [ref-output-stream (first (get-in caudex-impl [:op-stream-map (dbsp/-get-id op) :outputs]))
                  ref-data (last (get (:streams caudex-impl) ref-output-stream))
                  stream-data (into
                               {}
                               (map #(vector (-> % butlast vec) (last %)))
                               (state/getv c-state (dbsp/-get-id op)))]
              (when (not= stream-data ref-data)
                (dump-debug-data debug-data)
                (caudex.utils/circuit->map (assoc caudex-impl :circuit circuit))
                (throw
                 (ex-info
                  (str "mismatch in " (dbsp/-get-id op) " " stream-data " " ref-data)
                  {:tx tx})))))
          [caudex-impl c-state]))
      [caudex-impl c-state]
      transactions)))

(comment
  (def circuit
    (c/build-circuit
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
                 [(ground :not-accessible) ?accessible]))]))
  (let [transactions [[[:obj :object/desc "desc" 123 true]
                       [:obj :object/loc :loc 123 true]
                       [:player :object/desc "player" 123 true]
                       [:player :object/loc :loc-2 123 true]]
                      [[:player :object/loc :loc 123 true]
                       [:player :object/loc :loc-2 123 false]]]
        caudex-impl (c.impl/reify-circuit circuit)
        inline-impl (impl-inline/reify-circuit circuit)]
    (compare-states circuit caudex-impl inline-impl transactions))

 (def op (some #(when (= 'input-17094 (dbsp/-get-id %)) %) (g/nodes circuit)))
  
  (def zs (into (sset/sorted-set-by (wizard.zset/mk-comparator [1 0 2 3]))
                [[:accessible :player "player" true]
                 [:not-accessible :obj "desc" true]]))
  (sset/slice zs [:* :player :* :*] [:* :player :* :*]
              )
  (def root (.root zs))
  (.len root)
  (.keys root)
  (prn (last (.keys root)))
  (.searchFirst root [:* :player :* :*] (.comparator zs)))
