(ns wizard.circuit-inline-impl-test
  (:require [clojure.test :refer [deftest is]]
            [matcher-combinators.test]
            [caudex.circuit :as c]
            [wizard.circuit-impl-inline :as impl]
            [wizard.circuit-test-cases :as test-cases]
            [wizard.circuit.state :as c.state]))



(deftest run-cases
  (doseq [{:keys [query rules data case]} test-cases/test-cases]
    (println (str "Testing " case))
    (let [ccircuit (c/build-circuit query rules)
          circuit (eval `(impl/reify-circuit ~ccircuit))
          c-state (c.state/atom-state ccircuit)]
      (reduce
       (fn [c-state {:keys [tx output]}]
         (let [res (circuit c-state tx)]
           (when output
             (is (= res output)))
           c-state))
       c-state
       data))))


(comment
  (doseq [t [simple-select simple-join test-not-join test-disjoint-not-join test-preds test-or-join-3]]
    (clojure.test/run-test t)))
