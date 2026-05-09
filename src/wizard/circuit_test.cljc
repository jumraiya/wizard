(ns wizard.circuit-test
  (:require [clojure.test :refer [deftest is]]
            [caudex.circuit :as c]
            [matcher-combinators.test]
            [wizard.circuit-impl :as impl]
            [wizard.circuit-test-cases :as t]))


(deftest run-test-cases
  (doseq [{:keys [case query rules data]} t/test-cases]
    (println (str "Testing " case))
    (let [circuit (impl/reify-circuit (c/build-circuit query rules))]
     (reduce
      (fn [circuit {:keys [tx output]}]
        (let [res (circuit tx)]
          (when output
            (is (= res output)))
          circuit))
      circuit
      data))))
