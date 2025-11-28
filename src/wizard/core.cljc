(ns wizard.core
  (:require [caudex.circuit :as c]
            [wizard.circuit-impl :as c.impl]
            [caudex.utils :as c.utils]
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
                          (map vec))
                         output)
           retracts (into []
                          (comp
                           (filter #(false? (last %)))
                           (map butlast)
                           (map vec))
                          output)
           view (reduce
                 conj
                 (reduce
                  disj
                  view
                  retracts)
                 asserts)]
       (swap! circuits update id
              (fn [{:keys [diffs] :as data}]
                (assoc data :view view :diffs (conj diffs output))))
       (into
        tx
        (when-not (empty? output)
          (reduce
           #(into %1 (%2 asserts retracts view))
           []
           (get @subscriptions id))))))
   []
   @circuits))

(defn add-view
  "Creates and registers a materialized view from a Datalog query.

  Args:
    conn  - A DataScript connection
    id    - A unique identifier for this view
    query - A Datalog query in the form [:find ... :where ...]

  Options:
    :args  - Map of input arguments for the query (default: {})
    :rules - Vector of Datalog rules to use with the query (default: [])

  The view will be automatically maintained as transactions occur."
  [conn id query
   & {:keys [args rules] :or {args {} rules []}}]
  (when (not (ds/conn? conn))
    (let [msg (str "First argument should be a datascript connection, got " conn)]
      (throw #?(:clj (Exception. msg)
                :cljs (js/Error. msg)))))
  (let [tx-data (into (ds/datoms @conn :eavt)
                      (mapv #(vector ::c/input (key %) (val %) -1 true) args))
        ccircuit (c/build-circuit query rules)
        circuit (c.impl/reify-circuit ccircuit)
        view (into #{}
                   (comp
                    (filter #(true? (last %)))
                    (map butlast)
                    (map vec))
                   (circuit tx-data))]
    (swap! ccircuits assoc id ccircuit)
    (swap! circuits assoc id {:circuit circuit :view view :diffs []})
    (swap! subscriptions assoc id [])))


(defn deserialize+add-view
  "Creates and registers a materialized view from a serialized circuit EDN.

  Args:
    conn - A DataScript connection
    id   - A unique identifier for this view
    edn  - Serialized circuit representation in EDN format

  Options:
    :args - Map of input arguments for the circuit (default: {})

  This function is useful for loading pre-compiled circuits without re-building
  them from queries."
  [conn id edn & {:keys [args] :or {args {}}}]
  (let [ccircuit (c.utils/edn->circuit edn)
        circuit (c.impl/reify-circuit ccircuit)
        tx-data (into (ds/datoms @conn :eavt)
                      (mapv #(vector ::c/input (key %) (val %) -1 true) args))
        view (into #{}
                   (comp
                    (filter #(true? (last %)))
                    (map butlast)
                    (map vec))
                   (circuit tx-data))]
    (swap! ccircuits assoc id ccircuit)
    (swap! circuits assoc id {:circuit circuit :view view :diffs []})
    (swap! subscriptions assoc id [])))

(defn transact
  "Transacts data to the DataScript connection and updates all registered views.

  Args:
    conn    - A DataScript connection
    tx-data - Transaction data in DataScript format

  Returns:
    The transaction result from DataScript

  This function processes the transaction through all registered views and
  recursively applies any derived transactions until a fixed point is reached."
  [conn tx-data]
  (let [{:keys [tx-data] :as ret} (ds/transact! conn tx-data)
        new-tx (process-tx ::views tx-data)]
    (if (seq new-tx)
      (transact conn new-tx)
      ret)))


(defn get-view
  "Retrieves the current materialized view data for a given view ID.

  Args:
    id - The view identifier

  Returns:
    A set of tuples representing the current view state, or nil if the view
    doesn't exist."
  [id]
  (get-in @circuits [id :view]))

(defn subscribe-to-view
  "Subscribes a callback function to changes in a view.

  Args:
    id       - The view identifier to subscribe to
    callback - A function that will be called with (asserts, retracts, view)
               whenever the view changes

  The callback receives three arguments:
    - asserts:  Vector of tuples that were added to the view
    - retracts: Vector of tuples that were removed from the view
    - view:     The complete current view state"
  [id callback]
  (swap! subscriptions update id #(conj % callback)))

(defn add+subscribe-to-view
  "Creates a view and immediately subscribes a callback to it.

  Args:
    conn     - A DataScript connection
    id       - A unique identifier for the view
    callback - A function called with (asserts, retracts, view) on changes
    query    - A Datalog query in the form [:find ... :where ...]
    args     - Additional arguments passed to add-view (e.g., :args, :rules)

  This is a convenience function that combines add-view and subscribe-to-view."
  [conn id callback query & args]
  (apply add-view conn id query args)
  (subscribe-to-view id callback))

(defn reset-all
  "Resets all views and subscriptions to empty state.

  This clears all registered views, circuits, and subscription callbacks.
  Useful for testing or cleaning up state."
  []
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
