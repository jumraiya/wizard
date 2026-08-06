(ns wizard.config
  (:require
    [schema.core :as s]))


(s/defschema WizardConfig
             {:wizard/workspace-dir s/Str
              (s/optional-key :wizard/circuit-ns-prefix) s/Symbol
              (s/optional-key :wizard/target) (s/enum :clj :cljs)
              :wizard/circuits
              {s/Keyword {:wizard.circuit/name s/Str
                          :wizard.circuit/query [s/Any]
                          (s/optional-key :wizard.storage/type) (s/enum :wizard.storage/rocksdb :wizard.storage/lmdb)
                          (s/optional-key :wizard.circuit/rules) [s/Any]}}})


(defn ensure-config-valid
  [conf]
  (s/validate WizardConfig conf))
