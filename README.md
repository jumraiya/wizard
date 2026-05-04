# Wizard

A reactive view system for Datomic like databases that enables materialized views and reactive queries.
[![Clojars Project](https://img.shields.io/clojars/v/net.clojars.jumraiya/wizard.svg)](https://clojars.org/net.clojars.jumraiya/wizard)

## Rationale

Datomic and similar databases are awesome and provide a lot of features that are sorely missed in other systems, e.g. transaction log/history, graph like access patterns etc.

However, in my experience (and perhaps others) there can be a challenge to scale efficiently and maintain high performance. Yes in Datomic you can add peers and leverage the object cache. But the amount of segments you can cache locally depend on the data locality of your tenants. 

Incremental materialized views can address the above as well as provide a way to build state machines out of your database.

Wizard is a library that implements the above and is based on ideas from [DBSP](https://arxiv.org/abs/2203.16684).


## How it works

You give wizard a query plus any rules and wizard creates a "circuit" which is essentially the blueprint for an incremental materialized view. 

A circuit consumes datalog transactions in the EAVT format and updates the result set as needed.

Circuits can be attached to listeners (basically an event handler). In the future I will probably add functionality to connect to pub/sub systems.

For each circuit there are two artifacts created: an edn definition file, a clojure function generated from the latter which actually consumes the transactions and outputs the result.


## Usage

First we need to specify which database we are consuming data from. 

In this case Datomic,

```clojure
(require '[wizard.core :as w]
         '[datomic.api :as d])

(w/set-data-source! 
  (w/datomic-source (d/connect "datomic:dev://localhost:4334/mbrainz-1968-1973")))
```
similar adapters exist for Datalevin and Datascript.

A circuit can be constructed in two ways

During runtime, this might
```clojure
(require '[wizard.core :as w])

(w/add-view 
```


```clojure
(require '[wizard.core :as w])

(w/load
```



