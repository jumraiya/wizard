# Wizard

A reactive view system for DataScript databases that enables materialized views and reactive queries.
[![Clojars Project](https://img.shields.io/clojars/v/net.clojars.jumraiya/wizard.svg)](https://clojars.org/net.clojars.jumraiya/wizard)

## Overview

Wizard provides a way to create materialized views over DataScript databases that automatically update when the underlying data changes. Still a work in progress, only should be used with non production applications.
Supports
- standard queries including pattern, predicate, function, or-join, not-join clauses
- rules (Does not support recursive rules currently)

Does not support parameterized queries (WIP)

## Core Functions


### `transact`

Wrapper around DataScript's `transact!` that triggers view updates.

```clojure
(transact conn tx-data)
```

**Parameters:**
- `conn` - A DataScript connection
- `tx-data` - Transaction data (same format as DataScript)

**Returns:** Transaction result (same as DataScript's `transact!`)

**Important:** Use this instead of `ds/transact!` to ensure views are updated.


### `add-view`

Creates a new materialized view for a DataScript connection.

```clojure
(add-view conn id query & {:keys [args rules] :or {args {} rules []}})
```

**Parameters:**
- `conn` - A DataScript connection
- `id` - A unique identifier for this view
- `query` - A DataScript query (datalog)
- `rules` - Optional vector of datalog rules (default: `[]`)

**Example:**
```clojure
(require '[datascript.core :as ds]
         '[wizard.core :as wizard])

(def conn (ds/create-conn))

(wizard/add-view conn :my-view 
  '[:find ?person ?name 
    :where 
    [?person :person/name ?name]
    [?person :person/age ?age]
    [(> ?age 18)]])
```

### `get-view`

Retrieves the current materialized results of a view.

```clojure
(get-view id)
```

**Parameters:**
- `id` - The identifier of the view

**Returns:** A set containing the current query results

**Example:**
```clojure
(wizard/get-view :my-view)
;; => #{[1 "Alice"] [2 "Bob"]}
```

### `subscribe-to-view`

Registers a callback function to be called when a view changes.

```clojure
(subscribe-to-view id callback)
```

**Parameters:**
- `id` - The identifier of the view
- `callback` - A function that will be called with `(asserts retracts view)` when the view changes
  - `asserts` - New tuples added to the view
  - `retracts` - Tuples removed from the view  
  - `view` - The complete current view

**Example:**
```clojure
(wizard/subscribe-to-view :my-view
  (fn [asserts retracts view]
    (println "Added:" asserts)
    (println "Removed:" retracts)
    (println "Current view:" view)))
```
### `add+subscribe-to-view`

Convenience function that combines `add-view` and `subscribe-to-view`.

```clojure
(add+subscribe-to-view conn id callback query & args)
```

**Example:**
```clojure
(wizard/add+subscribe-to-view conn :my-view
  (fn [asserts retracts view]
    (println "View changed!"))
  '[:find ?e ?name :where [?e :person/name ?name]])
```


### `reset-all`

Clears all views and subscriptions. Useful for testing or cleanup.

```clojure
(reset-all)
```

**Subscribers can also return transaction data** to be automatically applied:
```clojure
(wizard/add+subscribe-to-view conn :low-stock-items
  (fn [asserts retracts view]
    ;; Return transaction data that will be automatically applied
    (for [[item-id name qty] asserts]
      [[:db/add -1 :reorder/item item-id]
       [:db/add -1 :reorder/quantity (* qty 3)]
       [:db/add -1 :reorder/status :pending]]))
  '[:find ?item ?name ?qty
    :where 
    [?item :item/name ?name]
    [?item :item/quantity ?qty]
    [(< ?qty 10)]])
```

**Reactive chains - one view's changes trigger another view:**
```clojure
;; Create a view for low stock items that auto-creates reorders
(wizard/add+subscribe-to-view
 conn :low-stock-items
 (fn [asserts _retracts _view]
   (mapv (fn [[idx [item-id _name qty]]]
           {:db/id (* -1 (inc idx))
            :reorder/item item-id
            :reorder/quantity (* qty 5)
            :reorder/status :pending})
         (map-indexed vector asserts)))
 '[:find ?item ?name ?qty
   :where
   [?item :item/name ?name]
   [?item :item/quantity ?qty]
   [(< ?qty 10)]])

;; Create a view for pending reorders that sends notifications
(wizard/add+subscribe-to-view conn :pending-reorders
  (fn [asserts retracts view]
    (doseq [[reorder item qty] asserts]
      (println "ALERT: Created reorder for item" item "quantity" qty)))
  '[:find ?reorder ?item ?qty
    :where
    [?reorder :reorder/item ?item]
    [?reorder :reorder/quantity ?qty]
    [?reorder :reorder/status :pending]])

;; Now when inventory drops:
(wizard/transact conn
  [[:db/add 101 :item/name "Widget A"]
   [:db/add 101 :item/quantity 25]])

;; Later, after sales...
(wizard/transact conn
  [[:db/add 101 :item/quantity 8]]) ; Below threshold of 10

;; This triggers a chain reaction:
;; 1. :low-stock-items view updates with Widget A
;; 2. Its subscriber creates a reorder with quantity 40 (8 * 5)
;; 3. :pending-reorders view updates with new reorder
;; 4. Its subscriber prints the reorder alert
```


## Notes

- Views are materialized and kept in memory, so they're fast to query but use memory
- The system automatically handles incremental updates - only changed portions of views are recalculated
- Views can depend on other views through subscriptions, creating reactive chains
- Always use `wizard/transact` instead of `ds/transact!` to ensure views are updated
