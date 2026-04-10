(ns wizard.circuit-test-cases)

(def test-cases
  [{:query '[:find ?a ?b
             :where
             [?a :attr-1 ?b]]
    :data [[[[1 :attr-1 2 123 true]
             [2 :attr-2 12 123 true]]
            #{[1 2 true]}]]
    :case "simple-select"}
   {:query '[:find ?a ?b
             :where
             [?a :attr-1 ?b]
             [?b :attr-2 12]]
    :data [{:tx [[1 :attr-1 2 123 true]
                 [2 :attr-2 12 123 true]
                 [3 :attr-1 4 123 true]
                 [4 :attr-2 10 123 true]]
            :output #{[1 2 true]}}
           {:tx [[2 :attr-2 12 124 false]
                 [4 :attr-2 10 124 false]
                 [4 :attr-2 12 124 true]]
            :output #{[1 2 false]
                      [3 4 true]}}]
    :case "simple-join"}
   {:query '[:find ?a ?c
             :where
             [?a :attr-1 ?b]
             [(> ?b 4)]
             [(* ?b 100) ?c]]
    :data [{:tx [[1 :attr-1 2 123 true]
                 [3 :attr-1 10 123 true]]
            :output #{[3 1000 true]}}]
    :case "test-preds"}
   {:query '[:find ?a
             :where
             [?a :attr-1 ?b]
             (not-join [?b]
                       [?b :attr-2 10])]
    :data [{:tx [[1 :attr-1 2 123 true]
                 [2 :attr-2 12 123 true]
                 [3 :attr-1 11 123 true]
                 [11 :attr-2 10 123 true]]
            :output #{[1 true]}}]
    :case "test-not-join"}
   {:query '[:find ?a ?b
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
    :data [{:tx [[1 :attr 10 123 true]
                 [1 :attr-2 :asd 123 true]]
            :output #{[1 :branch-1 true]}}
           {:tx [[2 :attr 10 124 true]]
            :output #{[2 :branch-3 true]}}]
    :case "test-or-join-3"}
   {:query '[:find ?a ?arg ?o ?det
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
    :data [{:tx [[:obj :object/description "desc" 123 true]
                 [:obj :object/detailed-description "detailed desc" 123 true]
                 [:obj :object/location :loc 123 true]
                 [:player :object/description "player" 123 true]]}
           {:tx [[:player :object/location :loc 123 true]]}
           {:tx [[:action-1 :action/type :move 124 true]
                 [:action-1 :action/arg "south" 124 true]]}
           {:tx [[:action-2 :action/type :inspect 124 true]
                 [:action-2 :action/arg "desc" 124 true]]}
           :output #{[:action-2 "desc" :obj "detailed desc" true]}
           {:tx [[:action-2 :action/inspect-processed? true 124 true]]}
           {:tx [[:player :object/location :loc 123 false]
                 [:player :object/location :loc-2 123 true]]}
           {:tx [[:action-3 :action/type :inspect 124 true]
                 [:action-3 :action/arg "desc" 124 true]]
            :output #{[:action-3 "desc" :not-found "no-description" true]}}]
    :case "test-or-join-5"}
   {:query '[:find ?o ?accessible
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
    :data [{:tx [[:obj :object/desc "desc" 123 true]
                 [:obj :object/loc :loc 123 true]
                 [:player :object/desc "player" 123 true]
                 [:player :object/loc :loc-2 123 true]]
            :output #{[:obj :not-accessible true]}}
           {:tx [[:player :object/loc :loc 123 true]
                 [:player :object/loc :loc-2 123 false]]
            :output #{[:obj :accessible true] [:obj :not-accessible false]}}]
    :case "test-disjoint-not-join"}
   {:query '[:find ?a ?b
             :in $ %
             :where
             [?a :attr 12]
             (rule ?a ?b)]
    :rules '[[(rule ?p ?q)
              [?p :attr-2 :a]
              [?q :attr-3 ?p]
              (nested-rule ?q)]
             [(nested-rule ?r)
              [(= ?r 10)]]]
    :data [{:tx [[1 :attr 12 123 true]
                 [1 :attr-2 :a 123 true]
                 [10 :attr-3 1 123 true]
                 [2 :attr 12 123 true]
                 [2 :attr-2 :a 123 true]
                 [11 :attr-3 2 123 true]]
            :output #{[1 10 true]}}]
    :case "test-nested-rules"}
   {:query '[:find ?c ?a
             :in $ %
             :where
             (rule ?a :const)
             [?c :attr ?a]]
    :rules '[[(rule ?c ?v)
              [?c :attr-2 ?v]]]
    :data [{:tx [[1 :attr 2 123 true]
                 [2 :attr-2 :const 123 true]
                 [3 :attr 4 123 true]]
            :output #{[1 2 true]}}]
    :case "test-rules-free-vars-1"}
   {:case "test-rules-free-vars-2"
    :query '[:find ?c ?a
             :in $ %
             :where
             (rule ?a :const)
             [?c :attr ?a]]
    :rules '[[(rule ?c ?v)
              [?c :attr-2 10]
              (or-join [?v]
                       [(= ?v :const)]
                       [(= ?v :val2)])]]
    :data [{:tx [[1 :attr 2 123 true]
                 [2 :attr-2 10 123 true] ;;matches
              ;; no match
                 [4 :attr 3 123 true]]
            :output #{[1 2 true]}}]}])
