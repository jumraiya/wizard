(ns wizard.views
  (:require
   [caudex.circuit :as c]
   [caudex.utils :as utils]
   [wizard.circuit-impl-inline :as impl]))

(defmacro query->view
  "Builds a circuit at compile time and emits code that returns
  {:circuit-fn ... :state ... :circuit ...}.
  The :circuit-fn is the specialized fn produced by reify-circuit;
  :state is a fresh atom-state matching :circuit (same op-ids).
  CLJS-only — embeds the circuit EDN as a literal in the output."
  ([query] `(query->view ~query nil))
  ([query rules]
   (let [[query rules] (mapv #(if (instance? clojure.lang.Cons %)
                                (second %) %)
                             [query rules])
         base (c/build-circuit query rules)
         edn (utils/circuit->edn base)]
     `(let [circuit# (caudex.utils/edn->circuit (quote ~edn))]
        {:circuit-fn (impl/reify-circuit ~base true)
         :state (wizard.circuit.state/atom-state circuit#)
         :circuit circuit#}))))

(defmacro defview
  ([view-name query] `(defview ~view-name ~query nil))
  ([view-name query rules]
   `(def ~view-name
      (query->view ~query ~rules))))
