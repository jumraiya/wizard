# Wizard

A reactive view system for DataScript databases that enables materialized views and reactive queries.
[![Clojars Project](https://img.shields.io/clojars/v/net.clojars.jumraiya/wizard.svg)](https://clojars.org/net.clojars.jumraiya/wizard)

## Overview

Wizard provides a way to create materialized views over DataScript databases that automatically update when the underlying data changes. Still a work in progress, only should be used with non production applications.

## Core Functions

### `add-view`

Creates a new materialized view for a DataScript connection.

```clojure
(add-view conn id query & {:keys [args rules] :or {args {} rules []}})
```

**Parameters:**
- `conn` - A DataScript connection
- `id` - A unique identifier for this view
- `query` - A DataScript query (datalog)
- `args` - Optional map of query arguments (default: `{}`)
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

### `transact`

Enhanced version of DataScript's `transact!` that triggers view updates.

```clojure
(transact conn tx-data)
```

**Parameters:**
- `conn` - A DataScript connection
- `tx-data` - Transaction data (same format as DataScript)

**Returns:** Transaction result (same as DataScript's `transact!`)

**Important:** Use this instead of `ds/transact!` to ensure views are updated.

**Example:**
```clojure
(wizard/transact conn 
  [[:db/add -1 :person/name "Charlie"]
   [:db/add -1 :person/age 25]])
```

### `reset-all`

Clears all views and subscriptions. Useful for testing or cleanup.

```clojure
(reset-all)
```

## Usage Example

```clojure
(require '[datascript.core :as ds]
         '[wizard.core :as wizard])

;; Create a DataScript connection
(def conn (ds/create-conn))

;; Create a view for adults
(wizard/add-view conn :adults
  '[:find ?person ?name
    :where 
    [?person :person/name ?name]
    [?person :person/age ?age]
    [(>= ?age 18)]])

;; Subscribe to changes
(wizard/subscribe-to-view :adults
  (fn [asserts retracts view]
    (when (seq asserts)
      (println "New adults:" asserts))
    (when (seq retracts)
      (println "Removed adults:" retracts))))

;; Add some data - this will trigger the view update
(wizard/transact conn
  [[:db/add -1 :person/name "Alice"]
   [:db/add -1 :person/age 25]
   [:db/add -2 :person/name "Bob"] 
   [:db/add -2 :person/age 16]])

;; Check the current view
(wizard/get-view :adults)
;; => #{[1 "Alice"]}  ; Only Alice is >= 18

;; Update Bob's age - this will add him to the view
(wizard/transact conn
  [[:db/add 2 :person/age 18]])

;; The subscription callback will be called with:
;; asserts: [[2 "Bob"]]
;; retracts: []
;; view: #{[1 "Alice"] [2 "Bob"]}
```

## Dependencies

- `datascript` - For the underlying database
- `caudex` - For the circuit-based reactive system (local dependency)

## Notes

- Views are materialized and kept in memory, so they're fast to query but use memory
- The system automatically handles incremental updates - only changed portions of views are recalculated
- Views can depend on other views through subscriptions, creating reactive chains
- Always use `wizard/transact` instead of `ds/transact!` to ensure views are updated
