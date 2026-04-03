(ns benchmark.datascript.wizard
  (:require
   [datascript.core :as d]
   [benchmark.bench :as bench]
   [wizard.circuit-impl-inline :as impl]
   [wizard.circuit.state :as c.state]))

(def schema
  {:id      {:db/unique :db.unique/identity}
   :follows {:db/valueType   :db.type/ref
             :db/cardinality :db.cardinality/many}
   :alias   {:db/cardinality :db.cardinality/many}})

;; Circuits compiled at macro expansion time

(def q1  (impl/query->circuit '[:find ?e
                                 :where [?e :name "Ivan"]]))

(def q2  (impl/query->circuit '[:find ?e ?a
                                 :where [?e :name "Ivan"]
                                        [?e :age ?a]]))

(def q3  (impl/query->circuit '[:find ?e ?a
                                 :where [?e :name "Ivan"]
                                        [?e :age ?a]
                                        [?e :sex :male]]))

(def q4  (impl/query->circuit '[:find ?e ?l ?a
                                 :where [?e :name "Ivan"]
                                        [?e :last-name ?l]
                                        [?e :age ?a]
                                        [?e :sex :male]]))

(def q5  (impl/query->circuit '[:find ?e ?l ?s ?al
                                 :where [?e :name "Anastasia"]
                                        [?e :age 35]
                                        [?e :last-name ?l]
                                        [?e :sex ?s]
                                        [?e :alias ?al]]))

(def qpred1 (impl/query->circuit '[:find ?e ?s
                                    :where [?e :salary ?s]
                                           [(> ?s 50000)]]))

(def qpred2 (impl/query->circuit '[:find ?e ?s
                                    :where [?e :salary ?s]
                                           [(> ?s 50000)]]))

(def follows-circuit
  (impl/query->circuit '[:find ?e ?e2
                          :in $ %
                          :where (follows ?e ?e2)]
                        '[[(follows ?x ?y)
                           [?x :follows ?y]]]))

;; Helpers

(defn- new-state [] (c.state/->AtomCircuitState (atom {})))

(defn- apply-delta [view delta]
  (reduce
   (fn [v row]
     (if (last row)
       (conj v (vec (butlast row)))
       (disj v (vec (butlast row)))))
   view
   delta))

(defn- build-view [circuit datoms]
  (apply-delta #{} (circuit (new-state) datoms)))

;; Pre-loaded data (used by query benches and reference queries)

(def *conn100k
  (delay
   (let [conn (d/create-conn schema)]
     (d/transact! conn @bench/*people20k)
     conn)))

(def *datoms100k
  (delay (d/datoms @@*conn100k :eavt)))

(def *ds-db100k
  (delay (d/db-with (d/empty-db schema) @bench/*people20k)))

;; Datascript reference benchmarks.
;; Each entry runs the equivalent d/q through bench/bench (for timing)
;; and captures the result for correctness comparison.
;; Add benchmarks only capture the result (no ds timing for the insert itself).

(defmacro ^:private ds-bench [result-sym & body]
  `(let [~result-sym (volatile! nil)
         timing# (bench/bench ~@body)]
     (assoc timing# :view @~result-sym)))

(def ds-benches
  {"add-1"           #(hash-map :view (d/q '[:find ?e
                                              :where [?e :name "Ivan"]]
                                           @*ds-db100k))
   "add-5"           #(hash-map :view (d/q '[:find ?e
                                              :where [?e :name "Ivan"]]
                                           @*ds-db100k))
   "add-all"         #(hash-map :view (d/q '[:find ?e
                                              :where [?e :name "Ivan"]]
                                           @*ds-db100k))
   "q1"              #(ds-bench r
                        (vreset! r (d/q '[:find ?e
                                          :where [?e :name "Ivan"]]
                                        @*ds-db100k)))
   "q2"              #(ds-bench r
                        (vreset! r (d/q '[:find ?e ?a
                                          :where [?e :name "Ivan"]
                                                 [?e :age ?a]]
                                        @*ds-db100k)))
   "q3"              #(ds-bench r
                        (vreset! r (d/q '[:find ?e ?a
                                          :where [?e :name "Ivan"]
                                                 [?e :age ?a]
                                                 [?e :sex :male]]
                                        @*ds-db100k)))
   "q4"              #(ds-bench r
                        (vreset! r (d/q '[:find ?e ?l ?a
                                          :where [?e :name "Ivan"]
                                                 [?e :last-name ?l]
                                                 [?e :age ?a]
                                                 [?e :sex :male]]
                                        @*ds-db100k)))
   "q5-shortcircuit" #(ds-bench r
                        (vreset! r
                          (set (map (fn [[e _ l _ s al]] [e l s al])
                                    (d/q '[:find ?e ?n ?l ?a ?s ?al
                                           :in $ ?n ?a
                                           :where [?e :name ?n]
                                                  [?e :age ?a]
                                                  [?e :last-name ?l]
                                                  [?e :sex ?s]
                                                  [?e :alias ?al]]
                                         @*ds-db100k "Anastasia" 35)))))
   "qpred1"          #(ds-bench r
                        (vreset! r (d/q '[:find ?e ?s
                                          :where [?e :salary ?s]
                                                 [(> ?s 50000)]]
                                        @*ds-db100k)))
   "qpred2"          #(ds-bench r
                        (vreset! r (d/q '[:find ?e ?s
                                          :where [?e :salary ?s]
                                                 [(> ?s 50000)]]
                                        @*ds-db100k)))})

;; --- Add benchmarks ---
;; Returns {:mean-ms ... :view <final-view-from-last-iteration>}

(defn bench-add-1 []
  (let [last-view (volatile! nil)
        timing    (bench/bench "add-1"
                   (let [state (new-state)
                         view  (volatile! #{})
                         conn  (d/create-conn schema)]
                     (doseq [p @bench/*people20k
                             tx [[:db/add (:db/id p) :name      (:name p)]
                                 [:db/add (:db/id p) :last-name (:last-name p)]
                                 [:db/add (:db/id p) :sex       (:sex p)]
                                 [:db/add (:db/id p) :age       (:age p)]
                                 [:db/add (:db/id p) :salary    (:salary p)]]]
                       (let [{:keys [tx-data]} (d/transact! conn [tx])]
                         (vswap! view apply-delta (q1 state tx-data))))
                     (vreset! last-view @view)))]
    (assoc timing :view @last-view)))

(defn bench-add-5 []
  (let [last-view (volatile! nil)
        timing    (bench/bench "add-5"
                   (let [state (new-state)
                         view  (volatile! #{})
                         conn  (d/create-conn schema)]
                     (doseq [p @bench/*people20k]
                       (let [{:keys [tx-data]} (d/transact! conn [p])]
                         (vswap! view apply-delta (q1 state tx-data))))
                     (vreset! last-view @view)))]
    (assoc timing :view @last-view)))

(defn bench-add-all []
  (let [last-view (volatile! nil)
        timing    (bench/bench "add-all"
                   (let [state (new-state)
                         {:keys [tx-data]} (d/transact (d/empty-db schema) @bench/*people20k)
                         view (apply-delta #{} (q1 state tx-data))]
                     (vreset! last-view view)))]
    (assoc timing :view @last-view)))

;; --- Retract benchmark (no result comparison — stateful across iterations) ---

(defn bench-retract-5 []
  (let [conn  (d/create-conn schema)
        _     (d/transact! conn @bench/*people20k)
        eids  (->> (d/datoms @conn :aevt :name) (map :e) shuffle)
        state (new-state)
        view  (volatile! (build-view q1 (d/datoms @conn :eavt)))]
    (bench/bench "retract-5"
      (doseq [eid eids]
        (let [{:keys [tx-data]} (d/transact! conn [[:db.fn/retractEntity eid]])]
          (vswap! view apply-delta (q1 state tx-data)))))))

;; --- Query benchmarks ---
;; Returns {:mean-ms ... :view <materialized-view>}

(defn bench-q1 []
  (let [view (build-view q1 @*datoms100k)]
    (assoc (bench/bench "q1" view) :view view)))

(defn bench-q2 []
  (let [view (build-view q2 @*datoms100k)]
    (assoc (bench/bench "q2" view) :view view)))

(defn bench-q3 []
  (let [view (build-view q3 @*datoms100k)]
    (assoc (bench/bench "q3" view) :view view)))

(defn bench-q4 []
  (let [view (build-view q4 @*datoms100k)]
    (assoc (bench/bench "q4" view) :view view)))

(defn bench-q5-shortcircuit []
  (let [view (build-view q5 @*datoms100k)]
    (assoc (bench/bench "q5-shortcircuit" view) :view view)))

(defn bench-qpred1 []
  (let [view (build-view qpred1 @*datoms100k)]
    (assoc (bench/bench "qpred1" view) :view view)))

(defn bench-qpred2 []
  (let [view (build-view qpred2 @*datoms100k)]
    (assoc (bench/bench "qpred2" view) :view view)))

(def benches
  {"add-1"           bench-add-1
   "add-5"           bench-add-5
   "add-all"         bench-add-all
   "retract-5"       bench-retract-5
   "q1"              bench-q1
   "q2"              bench-q2
   "q3"              bench-q3
   "q4"              bench-q4
   "q5-shortcircuit" bench-q5-shortcircuit
   "qpred1"          bench-qpred1
   "qpred2"          bench-qpred2})

(defn -main
  "clj -A:bench -M -m benchmark.wizard [bench-name ...]*"
  [& args]
  (let [args    (or args ())
        names   (or (not-empty args) (sort (keys benches)))
        _       (apply println "Wizard:" names)
        longest (last (sort-by count names))]
    (doseq [name names
            :let [f (benches name)]]
      (if (nil? f)
        (println "Unknown benchmark:" name)
        (let [{wizard-ms :mean-ms wizard-view :view} (f)
              {ds-ms :mean-ms ds-view :view}         ((ds-benches name))
              check (when (and wizard-view ds-view)
                      (if (= wizard-view ds-view) "OK"
                          (str "MISMATCH wizard=" (count wizard-view)
                               " ds=" (count ds-view))))]
          (println
           (bench/right-pad name (count longest))
           "  wizard:" (bench/left-pad (bench/round wizard-ms) 6) "ms/op"
           "  ds:" (bench/left-pad (if ds-ms (bench/round ds-ms) "n/a") 6) "ms/op"
           " " (or check ""))))))
  (shutdown-agents))

(comment
  (bench-add-1)
  (bench-add-5)
  (bench-add-all)
  (bench-retract-5)
  (bench-q1)
  (bench-q2)
  (bench-q3)
  (bench-q4)
  (bench-q5-shortcircuit)
  (bench-qpred1)
  (bench-qpred2))
