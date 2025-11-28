(ns wizard.circuit-test
  (:require [clojure.test :refer [deftest is]]
            [caudex.circuit :as c]
            [matcher-combinators.test]
            [wizard.circuit-impl :as impl]
            [matcher-combinators.matchers :as m]))

 (deftest simple-select
   (let [q '[:find ?a ?b
             :where
             [?a :attr-1 ?b]]
         circuit (impl/reify-circuit (c/build-circuit q))
         tx-data [[1 :attr-1 2 123 true]
                  [2 :attr-2 12 123 true]]
         output (circuit tx-data)]
     (is (match?
          #{[1 2 true]}
          output))))

(deftest simple-join
   (let [q '[:find ?a ?b
             :where
             [?a :attr-1 ?b]
             [?b :attr-2 12]]
         circuit (impl/reify-circuit (c/build-circuit q))
         tx-data [[1 :attr-1 2 123 true]
                  [2 :attr-2 12 123 true]
                  [3 :attr-1 4 123 true]
                  [4 :attr-2 10 123 true]]
         output-1 (circuit tx-data)
         output-2 (circuit [[2 :attr-2 12 124 false]
                            [4 :attr-2 10 124 false]
                            [4 :attr-2 12 124 true]])]
     (is (match?
          #{[1 2 true]}
          output-1))
     (is (match?
          #{[1 2 false]
            [3 4 true]}
          output-2))))


(deftest test-preds
  (let [q '[:find ?a ?c
            :where
            [?a :attr-1 ?b]
            [(> ?b 4)]
            [(* ?b 100) ?c]]
        c (c/build-circuit q)
        circuit (impl/reify-circuit c)
        tx-data [[1 :attr-1 2 123 true]
                 [3 :attr-1 10 123 true]]
        output (circuit tx-data)]
    (is (match?
         #{[3 1000 true]}
         output))))


(deftest test-not-join
  (let [q '[:find ?a
            :where
            [?a :attr-1 ?b]
            (not-join [?b]
                      [?b :attr-2 10])]
        ccircuit (c/build-circuit q)
        circuit (impl/reify-circuit ccircuit)
        tx-data [[1 :attr-1 2 123 true]
                 [2 :attr-2 12 123 true]
                 [3 :attr-1 11 123 true]
                 [11 :attr-2 10 123 true]]
        ;_ (impl/prn-circuit circuit)
        output (circuit tx-data)]
    (is (match?
         (m/equals
          #{[1 true]})
         output))))

(deftest test-or-join-3
  (let [q '[:find ?a ?b
            :where
            [?a :attr 10]
            (or-join [?a ?b]
                     (and
                      [?a :attr-2 :asd]
                      [(ground :branch-1) ?b])
                     (and
                      [?a :attr-2 :qwe]
                      [(ground :branch-2) ?b])
                     (and
                      (not-join [?a]
                                [?a :attr-2 :asd])
                      [(ground :branch-3) ?b]))]
        ccircuit (c/build-circuit q)
        circuit (impl/reify-circuit ccircuit)
        output (circuit [[1 :attr 10 123 true]
                         [1 :attr-2 :asd 123 true]])
        output-2 (circuit [[2 :attr 10 124 true]])]
    (is (match?
         #{[1 :branch-1 true]}
         output))
    (is (match?
         #{[2 :branch-3 true]}
         output-2))))

(deftest test-or-join-5
  (let [q '[:find ?a ?arg ?o ?det
            :where
            [?a :action/arg ?arg]
            [?a :action/type ?action-type]
            [?a :action/type :inspect]
            (not-join [?a]
                      [?a :action/inspect-processed? true])
            (or-join [?o ?arg ?det]
                     (and
                      [(not= ?arg "player")]
                      [?o :object/description ?arg]
                      [?o :object/detailed-description ?det]
                      [?p :object/description "player"]
                      [?p :object/location ?l]
                      (or-join [?l ?p ?o]
                               [?o :object/location ?p]
                               [?o :object/location ?l]))
                     (and
                      (or-join [?arg]
                               [(= ?arg "player")]
                               (not-join [?arg]
                                         [_ :object/description ?arg])
                               (and
                                [?o :object/description ?arg]
                                [(not= ?arg "player")]
                                (not-join [?o]
                                          [?p :object/description "player"]
                                          [?p :object/location ?l]
                                          (or-join [?l ?p ?o]
                                                   [?o :object/location ?p]
                                                   [?o :object/location ?l]))))
                      [(ground :not-found) ?o]
                      [(ground "no-description") ?det]))]
        ccircuit (c/build-circuit q)
        circuit (impl/reify-circuit ccircuit true)
        ;; circuit (impl/reify-circuit user/circuit true)
        _ (circuit
           [[:obj :object/description "desc" 123 true]
            [:obj :object/detailed-description "detailed desc" 123 true]
            [:obj :object/location :loc 123 true]
            [:player :object/description "player" 123 true]])
        _ (circuit
           [[:player :object/location :loc 123 true]])
        _ (circuit
           [[:action-1 :action/type :move 124 true]
            [:action-1 :action/arg "south" 124 true]])
        output (circuit
                [[:action-2 :action/type :inspect 124 true]
                 [:action-2 :action/arg "desc" 124 true]])
        _ (circuit
           [[:action-2 :action/inspect-processed? true 124 true]])
        _ (circuit
           [[:player :object/location :loc 123 false]
            [:player :object/location :loc-2 123 true]])
        output-2 (circuit
                  [[:action-3 :action/type :inspect 124 true]
                   [:action-3 :action/arg "desc" 124 true]])]
    (prn output output-2)
    (is (match?
         #{[:action-2 "desc" :obj "detailed desc" true]}
         output))

    (is (match?
         #{[:action-3 "desc" :not-found "no-description" true]}
         output-2))))

(deftest test-disjoint-not-join
  (let [q '[:find ?o ?accessible
            :where
            [?o :object/desc ?d]
            (not-join [?o]
                      [?o :object/desc "player"])
            (or-join [?o ?accessible]
                     (and
                      [?p :object/loc ?l]
                      [?p :object/desc "player"]
                      (or-join [?l ?p ?o]
                               [?o :object/loc ?p]
                               [?o :object/loc ?l])
                      [(ground :accessible) ?accessible])
                     (and
                      (not-join [?o]
                                [?p :object/loc ?l]
                                [?p :object/desc "player"]
                                (or-join [?l ?p ?o]
                                         [?o :object/loc ?p]
                                         [?o :object/loc ?l]))
                      [(ground :not-accessible) ?accessible]))]
        ccircuit (c/build-circuit q)
                                        ;_ (caudex.utils/prn-graph ccircuit)
        circuit (impl/reify-circuit ccircuit)
                                        ;not-accessible
        output (circuit
                [[:obj :object/desc "desc" 123 true]
                 [:obj :object/loc :loc 123 true]
                 [:player :object/desc "player" 123 true]
                 [:player :object/loc :loc-2 123 true]])
                                        ;accessible
        output-2 (circuit
                  [[:player :object/loc :loc 123 true]
                   [:player :object/loc :loc-2 123 false]])]
    (prn output output-2)
    (is (match? #{[:obj :not-accessible true]} output))
    (is (match? #{[:obj :accessible true] [:obj :not-accessible false]} output-2))))

(deftest test-nested-rules
  (let [q '[:find ?a ?b
            :in $ %
            :where
            [?a :attr 12]
            (rule ?a ?b)]
        rules '[[(rule ?p ?q)
                 [?p :attr-2 :a]
                 [?q :attr-3 ?p]
                 (nested-rule ?q)]
                [(nested-rule ?r)
                 [(= ?r 10)]]]
        tx-data [[1 :attr 12 123 true]
                 [1 :attr-2 :a 123 true]
                 [10 :attr-3 1 123 true]
                 [2 :attr 12 123 true]
                 [2 :attr-2 :a 123 true]
                 [11 :attr-3 2 123 true]]
        ccircuit (c/build-circuit q rules)
        circuit (impl/reify-circuit ccircuit)
        output (circuit tx-data)]
    (is (match? #{[1 10 true]} output))))

(deftest test-rules-free-vars
  (let [q '[:find ?c ?a
            :in $ %
            :where
            (rule ?a :const)
            [?c :attr ?a]]
        rules '[[(rule ?c ?v)
                 [?c :attr-2 ?v]]]
        ccircuit (c/build-circuit q rules)
        circuit (impl/reify-circuit ccircuit)
        tx-data [[1 :attr 2 123 true]
                 [2 :attr-2 :const 123 true]
                 [3 :attr 4 123 true]]
        output (circuit tx-data)

            ;; (rule ?a :const)
            ;; [?c :attr ?a]
        rules '[[(rule ?c ?v)
                 [?c :attr-2 10]
                 (or-join [?v]
                          [(= ?v :const)]
                          [(= ?v :val2)])]]
        ccircuit (c/build-circuit q rules)
        circuit (impl/reify-circuit ccircuit)
        tx-data [[1 :attr 2 123 true]
                 [2 :attr-2 10 123 true] ;;matches
                 ;; no match
                 [4 :attr 3 123 true]]
        output-2 (circuit tx-data)]
    (is (= #{[1 2 true]} output))
    (is (= #{[1 2 true]} output-2))))


