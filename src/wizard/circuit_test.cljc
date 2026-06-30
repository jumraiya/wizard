(ns wizard.circuit-test
  (:require [clojure.test :refer [deftest is testing]]
            #?(:clj [caudex.circuit :as c])
            [caudex.utils :as c.utils]
            #?(:clj [matcher-combinators.test])
            [wizard.circuit.state :as state]
            [wizard.circuit-impl-inline :as impl]
            [wizard.circuit-impl-inline-fns]
            [wizard.views :as v :include-macros true]
            [wizard.circuit-test-cases :as t])
  #?(:cljs (:require-macros [wizard.circuit-test :refer [gen-test-cases]])))

#?(:clj
   (defmacro gen-test-cases []
     (require 'caudex.circuit 'wizard.circuit-test-cases)
     (let [build-circuit (resolve 'caudex.circuit/build-circuit)
           cases @(resolve 'wizard.circuit-test-cases/test-cases)]
       `(do
          ~@(for [{:keys [case query rules data]} cases]
              (let [base (build-circuit query rules)
                    edn (clojure.edn/read-string
                         (pr-str (c.utils/circuit->edn base)))
                    test-name (symbol (str "test-" case))]
                `(deftest ~test-name
                   (println (str "Testing " ~case))
                   (let [circuit# (impl/edn->circuit ~edn)
                         c-state# (state/atom-state (c.utils/edn->circuit (quote ~edn)))]
                     (reduce
                      (fn [circ# {tx# :tx output# :output}]
                        (let [res# (circ# c-state# tx#)]
                          (when output#
                            (is (= res# output#)))
                          circ#))
                      circuit#
                      '~data)))))))))

#?(:clj
   (deftest run-test-cases
     (doseq [{:keys [case query rules data]} t/test-cases]
       (testing case
               (let [base (c/build-circuit query rules)
                     circuit (eval `(impl/reify-circuit ~base))
                     c-state (state/atom-state base)]
                 (reduce
                  (fn [circ {:keys [tx output]}]
                    (let [res (circ c-state tx)]
                      (when output
                        (is (= res output)))
                      circ))
                  circuit
                  data)))))
   :cljs
   (gen-test-cases))

#?(:cljs
     (deftest query-view-macro
       (let [{:keys [circuit-fn state circuit]} (v/query->view
                                                 [:find ?a ?b
                                                  :where
                                                  [?a :attr-1 ?b]])
             res (circuit-fn state [[1 :attr-1 2 123 true]
                                    [2 :attr-2 12 123 true]])]
         (is (some? circuit))
         (is (some? state))
         (is (= #{[1 2 true]} res)))))

(comment
  )
