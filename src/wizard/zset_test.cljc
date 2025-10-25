(ns wizard.zset-test
  (:require
   [wizard.zset :as z]
   [clojure.test :refer [deftest is]]
   [me.tonsky.persistent-sorted-set :as sset]))

(deftest add-zs-test
  (let [vars-1 '[?a ?b]
        vars-2 '[?b ?q]
        init-fn (z/mk-zset-init-fn-for-join vars-1 vars-2)
        set-1 (into (init-fn)
                    [[:a "123" true] [:b "123" true]])
        set-2 (into (init-fn)
                    [[:c "123" false] [:a "123" false]])]
    ;; New joined rows should be conjable
    (is (= set-1 #{[:a "123" true] [:b "123" true]}))
    ;; Rows with opposite values should cancel out
    (is (= #{[:b "123" true]
             [:c "123" false]}
           (z/add-zsets set-1 set-2)))
    ;; sets without a custom comparator should work correctly
    (is (empty?
         (z/add-zsets
          (sset/sorted-set [:a 12 true])
          (sset/sorted-set [:a 12 false]))))
    ;; no duplicates should be allowed
    (is (= set-1
           (z/add-zsets set-1 (into (init-fn) [[:a "123" true]]))))))

(deftest lookup-zset
  (let [vars-1 '[?a ?b ?c]
        vars-2 '[?p ?b ?r]
        init-fn (z/mk-zset-init-fn-for-join vars-1 vars-2)
        zset (into (init-fn) [[23 "asd" :ed true]
                              [21 "efw" :asd true]
                              [54 "asd" :ded false]])
        lookup-key [:* "asd" :* :*]]
    (is (= #{[23 "asd" :ed true] [54 "asd" :ded false]}
           (sset/slice zset lookup-key lookup-key)))))
