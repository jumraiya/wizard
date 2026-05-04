(ns wizard.core
  (:require [caudex.circuit :as c]
            [wizard.config :as config]
            [wizard.circuit-impl :as c.impl]
            [caudex.utils :as c.utils]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [wizard.data-source :as d.src]
            [wizard.circuit-impl-inline :as impl-inline]
            [clojure.pprint :as pp]
            [clojure.walk :as walk]
            #?(:clj [wizard.lmdb.circuit-state :as l])
            [datomic.api :as d]
            #?(:clj [wizard.rocksdb.circuit-state :as r])
            [wizard.circuit.state :as c.state]
            [datascript.core :as ds])
  (:import (java.net URI)
           (java.nio.file Path Paths Files LinkOption)
           (java.nio.file.attribute PosixFilePermissions)))

(defonce ^:private data-source (atom nil))

(defonce ^:private circuits (atom {}))

(defonce ^:private subscriptions (atom {}))

(defonce ^:private ccircuits (atom {}))

#?(:clj (set! *warn-on-reflection* true))

(defn- process-tx [_id tx-data]
  (reduce
   (fn [tx [id {:keys [circuit view state]}]]
     (let [output (if (some? state)
                    (circuit state tx-data)
                    (circuit tx-data))
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

(defn set-data-source! [source]
  (reset! data-source source))

(defn add-view
  [id query
   & {:keys [storage-type args data-dir rules sync?] :or {args {} rules [] sync? true}}]
  (let [ccircuit (c/build-circuit query rules)
        compiled-circuit (c.impl/reify-circuit ccircuit)
        c-state #?(:clj (case storage-type
                          :wizard.storage/lmdb
                          (l/lmdb-state data-dir ccircuit)
                          :wizard.storage/rocksdb
                          (r/rocksdb-state data-dir ccircuit)
                          (c.state/atom-state ccircuit))
                   :cljs (c.state/atom-state ccircuit))]
    (swap! ccircuits assoc id ccircuit)
    (swap! circuits assoc id {:circuit compiled-circuit :state c-state})
    (swap! subscriptions assoc id [])
    (when sync?
      (future
        (loop [datoms (d.src/datoms @data-source :eavt)]
          (let [to-process (take 1000 datoms)
                {:keys [circuit state]} (get @circuits id)]
            (circuit state to-process)
            (if-let [remaining (seq (drop 1000 datoms))]
              (recur remaining)
              (c.state/get-view state))))))))


(defn get-last-processed-tx [circuit-id]
  (c.state/get-last-processed-tx
   (get-in @circuits [circuit-id :state])))



(defn sync-view
  ([circuit-id]
   (sync-view circuit-id @data-source))
  ([circuit-id data-source]
   (sync-view circuit-id data-source (get-last-processed-tx circuit-id)))
  ([circuit-id data-source last-processed]
   (future
     (loop [datoms (if last-processed
                     (d.src/datoms-since-tx-id data-source last-processed)
                     (d.src/datoms data-source :eavt))]
       (let [to-process (take 1000 datoms)
             to-process (if (= :datomic (d.src/get-source-type data-source))
                          (mapv
                           (fn [[e a v tx added?]]
                             [e (d/ident (d/db (-> data-source :ctx :conn)) a) v tx added?])
                           to-process)
                          to-process)
             {:keys [circuit state]} (get @circuits circuit-id)]
         (circuit state to-process)
         (if-let [remaining (seq (drop 1000 datoms))]
           (recur remaining)
           (c.state/get-view state)))))))

(defn add-compiled-view [{:keys [id circuit compiled-circuit data-dir storage-type]}
                         & {:keys [args] :or {args {}}}]
  (let [c-state #?(:clj (case storage-type
                          :wizard.storage/lmdb
                          (l/lmdb-state data-dir circuit)
                          :wizard.storage/rocksdb
                          (r/rocksdb-state data-dir circuit)
                          (c.state/atom-state circuit))
                   :cljs (c.state/atom-state circuit))]
    (swap! circuits assoc id {:circuit compiled-circuit :state c-state})
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
  [tx]
  (assert (some? @data-source) "No data source set!")
  (let [{:keys [tx-data] :as ret} (d.src/transact @data-source tx)
        new-tx (process-tx ::views tx-data)]
    (if (seq new-tx)
      (transact new-tx)
      ret)))


(defn get-view
  "Retrieves the current materialized view data for a given view ID.

  Args:
    id - The view identifier

  Returns:
    A set of tuples representing the current view state, or nil if the view
    doesn't exist."
  [id]
  (c.state/get-view (get-in @circuits [id :state])))

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

(def datomic-source d.src/datomic-source)

(defn load-from-conf [{:wizard/keys [workspace-dir circuits] :as conf}]
  (config/ensure-config-valid conf)
  (let [main-path (Paths/get (URI/create (str "file://" workspace-dir)))
        edn-path (.resolve ^Path main-path "definitions")
        circuits-path (.resolve ^Path main-path "circuits")
        data-path (.resolve ^Path main-path "data")]
    (doseq [path [main-path edn-path data-path circuits-path]]
      (Files/createDirectories
       path (into-array
             [(PosixFilePermissions/asFileAttribute
               (PosixFilePermissions/fromString "rwxr-xr--"))])))
    (doseq [[c-name {:wizard.circuit/keys [query rules] :wizard.storage/keys [type]}]
            circuits]
      (let [c-data-path (.resolve ^Path data-path (name c-name))
            c-edn-path (.resolve ^Path edn-path (str (name c-name) ".edn"))
            data-exists? (Files/exists c-data-path (into-array LinkOption []))
            edn-exists? (Files/exists c-edn-path (into-array LinkOption []))
            edn-path-str (-> c-edn-path (.toAbsolutePath) (.toString))
            data-path-str (-> c-data-path (.toAbsolutePath) (.toString))
            circuit (if edn-exists?
                      (c.utils/edn->circuit (edn/read-string (slurp edn-path-str)))
                      (c/build-circuit query rules))]
        (when (and data-exists? (not edn-exists?))
          (throw (ex-info (str "Circuit data found but not the definition! Please delete " c-data-path) {:circuit c-name})))
        (when (not edn-exists?)
          (spit edn-path-str (pr-str (c.utils/circuit->edn circuit))))
        (when (not data-exists?)
          (Files/createDirectories
           data-path (into-array
                      [(PosixFilePermissions/asFileAttribute
                        (PosixFilePermissions/fromString "rwxr-xr--"))])))
        (add-compiled-view
         {:id c-name
          :circuit circuit
          :compiled-circuit (eval `(impl-inline/reify-circuit ~circuit))
          :storage-type type
          :data-dir data-path-str})))))


(comment
  (do
    (def movie-schema [{:db/ident :movie/title
                        :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/doc "The title of the movie"}

                       {:db/ident :movie/genre
                        :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/doc "The genre of the movie"}

                       {:db/ident :movie/release-year
                        :db/valueType :db.type/long
                        :db/cardinality :db.cardinality/one
                        :db/doc "The year the movie was released in theaters"}])

    (def first-movies [{:movie/title "The Goonies"
                        :movie/genre "action/adventure"
                        :movie/release-year 1985}
                       {:movie/title "Commando"
                        :movie/genre "action/adventure"
                        :movie/release-year 1985}
                       {:movie/title "Repo Man"
                        :movie/genre "punk dystopia"
                        :movie/release-year 1984}])
    (def uri "datomic:mem://mbrainz-1968-1973")
    (d/create-database uri)
    (def conn (d/connect uri))
    @(d/transact conn movie-schema)
    @(d/transact conn first-movies))

  (d/q '[:find ?title
         :where
         [?movie :movie/title ?title]
         [?movie :movie/genre "action/adventure"]
         [?movie :movie/release-year ?year]
         [(>= ?year 1980)]]
       (d/db conn))
  (def d-source (wizard.core/datomic-source conn))
  (wizard.core/set-data-source! d-source)
  (wizard.core/load-from-conf config)
  (wizard.data-source/get-source-type d-source)
  (wizard.core/sync-view :action-movies-1980s d-source)
  (wizard.core/get-view :action-movies-1980s)
  (d/transact
   conn
   [{:movie/title "Predator"
     :movie/genre "action/adventure"
     :movie/release-year 1987}])
  (wizard.core/sync-view :action-movies-1980s data-source)
  ;; include predator
  (wizard.core/get-view :action-movies-1980s))
