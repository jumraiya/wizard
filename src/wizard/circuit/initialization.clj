(ns wizard.circuit.initialization
  (:require [caudex.graph :as g]
            [caudex.circuit :as c]
            [caudex.dbsp :as dbsp]
            [caudex.utils :as utils]))


(defn- sort-nodes [circuit nodes]
  (let [sorted-nodes (into {}
                           (map-indexed #(vector (dbsp/-get-id %2) %1))
                           (utils/topsort-circuit circuit))]
    (sort-by #(get sorted-nodes (dbsp/-get-id %)) nodes)))

#trace
 (defn- find-subgraph
   ([circuit op]
    (find-subgraph circuit [op] (mapv :src (g/in-edges circuit op))))
   ([circuit nodes layer]
    (let [{:keys [terminal non-terminal]}
          (reduce
           #(if (or (= :integrate (dbsp/-get-op-type %2))
                    (some (fn [p]
                            (when (= :root (dbsp/-get-op-type p))
                              p))
                          (mapv :src (g/in-edges circuit %2))))
              (update %1 :terminal conj %2)
              (update %1 :non-terminal conj %2))
           {:terminal [] :non-terminal []}
           layer)
          next-layer (reduce #(into %1 (map :src) (g/in-edges circuit %2)) [] non-terminal)
          nodes (into (into terminal non-terminal) nodes)]
      (if (seq next-layer)
        (sort-nodes
         circuit
         (find-subgraph circuit nodes next-layer))
        nodes))))

(comment
  (spit "/tmp/circuit.edn"
        (caudex.utils/circuit->edn
         (c/build-circuit '[:find ?o ?accessible
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
                                      [(ground :not-accessible) ?accessible]))])))

  (wizard.circuit.debug/dump-circuit circuit)
  (prn
   (let [node
         (some #(when (= 'integrate-78890
                         (dbsp/-get-id %)) %)
               (g/nodes circuit))]
     (into []
           (map dbsp/-get-id)
           (find-subgraph circuit node))))

  (def circuit
    (caudex.utils/edn->circuit
     (clojure.edn/read-string
      (slurp "/tmp/circuit.edn")))))
