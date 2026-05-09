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

For each circuit there are two artifacts created: a definition of the structure in edn form and a data directory which contains the durable state of the circuit.


## Usage

```clojure
;; Setup datomic database
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
    (datomic.api/create-database uri)
    (def conn (datomic.api/connect uri))
    @(datomic.api/transact conn movie-schema)
    @(datomic.api/transact conn first-movies))

  (def query
    '[:find ?title
      :where
      [?movie :movie/title ?title]
      [?movie :movie/genre "action/adventure"]
      [?movie :movie/release-year ?year]
      [(>= ?year 1980)]])

  (def config
    {:wizard/workspace-dir "/tmp/my-circuits"
     :wizard/circuits
     {:action-movies-1980s
      {:wizard.circuit/name "1980s Action"
       :wizard.storage/type :wizard.storage/rocksdb
       :wizard.circuit/query query}}})

  (def d-source (wizard.core/datomic-source conn))
  (wizard.core/load-from-conf config)
  (wizard.core/sync-view :action-movies-1980s d-source)

 (assert
  (= (set (wizard.core/get-view :action-movies-1980s))
     (d/q query (d/db conn))))
```



