(ns wizard.data-source
  (:require
   [datascript.core :as ds]
   [datomic.api :as d]
   [datalevin.core :as dl]))

(defprotocol DataSource
  (transact [this tx-data])
  (datoms [this index] [this index c1] [this index c1 c2] [this index c1 c2 c3])
  (datoms-since-tx-id [this tx-id]))

(defrecord DatalevinSource [ctx]
  DataSource
  (transact [_this tx-data]
    (dl/transact! (:conn ctx) tx-data))
  (datoms [_this index]
    (dl/datoms (dl/db (:conn ctx)) index))
  (datoms [_this index c1]
    (dl/datoms (dl/db (:conn ctx)) index c1))
  (datoms [_this index c1 c2]
    (dl/datoms (dl/db (:conn ctx)) index c1 c2))
  (datoms [_this index c1 c2 c3]
    (dl/datoms (dl/db (:conn ctx)) index c1 c2 c3)))

(defn datalevin-source [db-conn]
  (->DatalevinSource {:conn db-conn}))


(defrecord DataScriptSource [ctx]
  DataSource
  (transact [_this tx-data]
    (ds/transact! (:conn ctx) tx-data))
  (datoms [_this index]
    (ds/datoms @(:conn ctx) index))
  (datoms [_this index c1]
    (ds/datoms @(:conn ctx) index c1))
  (datoms [_this index c1 c2]
    (ds/datoms @(:conn ctx) index c1 c2))
  (datoms [_this index c1 c2 c3]
    (ds/datoms @(:conn ctx) index c1 c2 c3)))

(defn datascript-source [db-conn]
  (->DataScriptSource {:conn db-conn}))

(defrecord DatomicSource [ctx]
  DataSource
  (transact [_this tx-data]
    @(d/transact (:conn ctx) tx-data))
  (datoms [_this index]
    (d/datoms (d/db (:conn ctx)) index))
  (datoms [_this index c1]
    (d/datoms (d/db (:conn ctx)) index c1))
  (datoms [_this index c1 c2]
    (d/datoms (d/db (:conn ctx)) index c1 c2))
  (datoms [_this index c1 c2 c3]
    (d/datoms (d/db (:conn ctx)) index c1 c2 c3))
  (datoms-since-tx-id [_ tx-id]
    (let [t (d/tx->t tx-id)]
      (d/datoms (d/since (d/db (:conn ctx)) t) :eavt))))

(defn datomic-source [db-conn]
  (->DatomicSource {:conn db-conn}))

(defn get-source-type [source]
  (cond
    (= "class wizard.data_source.DatomicSource" (str (type source))) :datomic
    (instance? DataScriptSource source) :datascript
    (instance? DatalevinSource source) :datalevin))
