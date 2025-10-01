(ns wizard.datomic-test
  (:require
   [wizard.examples.adventure :as adv]
   [datomic.api :as d]))

(def db-uri "datomic:mem://hello")

(def adv-db-uri "datomic:mem://adventure")

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

(comment
  (d/create-database db-uri)
  (def conn (d/connect db-uri))
  @(d/transact conn movie-schema)
  @(d/transact conn first-movies)

  (d/delete-database adv-db-uri)
  (d/create-database adv-db-uri)
  (def adv-conn (d/connect adv-db-uri))
  @(d/transact adv-conn adv/datomic-schema)

  (run!
   (fn [ent-type]
     (d/transact adv-conn
                 (get adv/datomic-init-data ent-type)))
   [:locations :exits])
  (run!
   (fn [ent]
     (d/transact adv-conn [ent]))
   (get adv/datomic-init-data :objects))

  (d/transact adv-conn [(second (get adv/datomic-init-data :objects))])

  (d/q '[:find ?e ?v ?l
         :where
         [?e :object/description ?v]
         [?e :object/location ?l]]
       (d/db adv-conn))

  (d/q '[:find ?p ?l
         :where
                        ;[?p :object/description "player"]
         [?p :object/location ?l]]
       (d/db adv-conn))

  (d/pull (d/db adv-conn) [:db/id] :player)
  (d/transact adv-conn [[:db/add :player :object/location :white-room]])
  (d/q '[:find ?o ?det
         :in $ %
         :where
         (lookup-object ?o "safe" ?det)]
       (d/db adv-conn)
       adv/rules))
