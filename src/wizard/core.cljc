(ns wizard.core
  (:require [caudex.circuit :as c]
            [wizard.circuit-impl :as c.impl]
                                        ;[caudex.impl.circuit :as c.impl]
            [datascript.core :as ds]))

(defonce ^:private circuits (atom {}))

(defonce ^:private subscriptions (atom {}))

(defonce ^:private ccircuits (atom {}))


(defn- process-tx [_id tx-data]
  (reduce
   (fn [tx [id {:keys [circuit view]}]]
     (let [output (circuit tx-data)
           asserts (into []
                         (comp
                          (filter #(true? (last %)))
                          (map butlast)
                          (map vec)
                          #_(filter #(true? (val %)))
                          #_(map key))
                         output)
           retracts (into []
                          (comp
                           (filter #(false? (last %)))
                           (map butlast)
                           (map vec)
                           #_(filter #(false? (val %)))
                           #_(map key))
                          output)
           view (reduce
                 conj
                 (reduce
                  disj
                  view
                  retracts)
                 asserts)
           #_(reduce
              (fn [view [row add?]]
                (if add?
                  (conj view row)
                  (disj view row)))
              view
              output)]
       (swap! circuits update id
              (fn [{:keys [diffs] :as data}]
                (assoc data :view view :diffs (conj diffs output) :circuit circuit)))
       (into
        tx
        (when-not (empty? output)
          (reduce
           #(into %1 (%2 asserts retracts view))
           []
           (get @subscriptions id))))))
   []
   @circuits))

(defn add-view [conn id query
                & {:keys [args rules] :or {args {} rules []}}]
  (when (not (ds/conn? conn))
    (let [msg (str "First argument should be a datascript connection, got " conn)]
      (throw #?(:clj (Exception. msg)
                :cljs (js/Error. msg)))))
  #_(ds/listen! conn ::views
              (fn [{:keys [tx-data]}]
                (process-tx id tx-data)))
  (let [tx-data (into (ds/datoms @conn :eavt)
                      (mapv #(vector :c/input (key %) (val %) -1 true) args))
        ccircuit (c/build-circuit query rules)
        circuit (c.impl/reify-circuit ccircuit)
        #_(-> ccircuit
              (c.impl/reify-circuit)
              (c.impl/step tx-data))
        view (into #{}
                   (comp
                    (filter #(true? (last %)))
                    (map butlast)
                    (map vec)
                    #_(filter #(true? (val %)))
                    #_(map key))
                   (circuit tx-data)
                   #_(-> circuit c.impl/get-output-stream last))]
    (swap! ccircuits assoc id ccircuit)
    (swap! circuits assoc id {:circuit circuit :view view :diffs []})
    (swap! subscriptions assoc id [])))

(defn transact [conn tx-data]
  (let [{:keys [tx-data] :as ret} (ds/transact! conn tx-data)
        new-tx (process-tx ::views tx-data)]
    (if (seq new-tx)
      (transact conn new-tx)
      ret)))


(defn get-view [id]
  (get-in @circuits [id :view]))

(defn subscribe-to-view [id callback]
  (swap! subscriptions update id #(conj % callback)))

(defn add+subscribe-to-view [conn id callback query & args]
  (apply add-view conn id query args)
  (subscribe-to-view id callback))

(defn reset-all []
  (reset! subscriptions {})
  (reset! circuits {}))


(comment
  (def conn (ds/create-conn))

  (add-view conn :test '[:find ?a :where [?a :attr-1 ?b] [?b :attr-2 "asd"]])

  (ds/transact!
   conn
   [[:db/add 54 :attr-1 65]
    [:db/add 65 :attr-2 "asd"]])

  (-> @circuits :wizard.examples.adventure/inspect-action :circuit caudex.utils/circuit->map)
  (ds/transact!
   conn
   [[:db/add 65 :attr-2 "cdv"]
    [:db/retract 65 :attr-2 "asd"]
    [:db/add 1 :attr-1 2]
    [:db/add 2 :attr-2 "asd"]]))
