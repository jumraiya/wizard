(ns wizard.config
  (:require [schema.core :as s]))


(s/defschema WizardConfig
  {:wizard/workspace-dir s/Str
   :wizard/circuits
   {s/Keyword {:wizard.circuit/name s/Str
               :wizard.circuit/query [s/Any]
               :wizard.storage/type (s/enum :wizard.storage/rocksdb :wizard.storage/lmdb)
               (s/optional-key :wizard.circuit/rules) [s/Any]}}})


(defn ensure-config-valid [conf]
  (s/validate WizardConfig conf))
