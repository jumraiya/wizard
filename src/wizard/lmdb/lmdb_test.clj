(ns wizard.lmdb.lmdb-test
  (:require [clojure.test :refer [deftest is testing]]
            [wizard.lmdb.core :as lmdb])
  (:import (java.nio.file Files)
           (java.io File)))

(defn- temp-dir ^File []
  (.toFile (Files/createTempDirectory "lmdb-test" (make-array java.nio.file.attribute.FileAttribute 0))))

;; ---- Encode / decode roundtrip (no LMDB required) ----

(deftest encode-decode-types
  (testing "boolean"
    (is (= [true]  (lmdb/decode (lmdb/encode [true]))))
    (is (= [false] (lmdb/decode (lmdb/encode [false])))))

  (testing "long / integer"
    (doseq [n [0 1 -1 42 -42 Long/MAX_VALUE Long/MIN_VALUE]]
      (is (= [n] (lmdb/decode (lmdb/encode [n]))) (str "long: " n))))

  (testing "double"
    (doseq [x [0.0 1.0 -1.0 3.14159 -2.718 Double/MAX_VALUE Double/MIN_VALUE]]
      (is (= [x] (lmdb/decode (lmdb/encode [x]))) (str "double: " x))))

  (testing "float"
    (doseq [x [(float 0.0) (float 1.5) (float -1.5) (float 3.14)]]
      (is (= [x] (lmdb/decode (lmdb/encode [x]))) (str "float: " x))))

  (testing "string"
    (doseq [s ["" "hello" "hello world" "ns/foo"]]
      (is (= [s] (lmdb/decode (lmdb/encode [s]))) (str "string: " s))))

  (testing "keyword"
    (doseq [k [:foo :bar :ns/foo :ns/bar]]
      (is (= [k] (lmdb/decode (lmdb/encode [k]))) (str "keyword: " k))))

  (testing "symbol"
    (doseq [s ['foo 'bar 'ns/foo 'ns/bar]]
      (is (= [s] (lmdb/decode (lmdb/encode [s]))) (str "symbol: " s))))

  (testing "mixed vector"
    (is (= [:op/id 42 :attr true]
           (lmdb/decode (lmdb/encode [:op/id 42 :attr true])))))

  (testing "symbol + long + keyword + boolean"
    (is (= ['integrate-123 99 :wt false]
           (lmdb/decode (lmdb/encode ['integrate-123 99 :wt false]))))))

(deftest long-sort-order
  ;; encode([n]) bytes must be in unsigned ascending order for sorted long values
  (let [longs [-10 -1 0 1 10 100]
        encoded (map #(lmdb/encode [%]) longs)]
    (is (= encoded (sort #(java.util.Arrays/compareUnsigned %1 %2) encoded))
        "longs should be stored in ascending byte order")))

(deftest encode-prefix-property
  ;; encode([v1 v2]) must be a byte-prefix of encode([v1 v2 v3])
  (doseq [[short long] [[[:foo 1] [:foo 1 :bar]]
                        [[:ns/k 42] [:ns/k 42 true]]
                        [['my-sym] ['my-sym :attr 99]]]]
    (let [short-bs (lmdb/encode short)
          long-bs  (lmdb/encode long)
          n        (alength short-bs)]
      (is (<= n (alength long-bs))
          (str "short key should be shorter: " short " vs " long))
      (is (java.util.Arrays/equals short-bs 0 n long-bs 0 n)
          (str "encode(" short ") should be a byte prefix of encode(" long ")")))))

;; ---- LMDB put / get / delete ----

(deftest put-get-delete
  (let [conn (lmdb/open-db (temp-dir) "test")]
    (testing "keyword key + true value"
      (lmdb/put conn [:my/key] true)
      (is (= true (lmdb/get-val conn [:my/key]))))

    (testing "mixed key + false value"
      (lmdb/put conn [:op-id 42 :attr] false)
      (is (= false (lmdb/get-val conn [:op-id 42 :attr]))))

    (testing "symbol key"
      (lmdb/put conn ['integrate-99 1 :attr] false)
      (is (= false (lmdb/get-val conn ['integrate-99 1 :attr]))))

    (testing "missing key returns nil"
      (is (nil? (lmdb/get-val conn [:does-not-exist]))))

    (testing "overwrite value"
      (lmdb/put conn [:flag] true)
      (lmdb/put conn [:flag] false)
      (is (= false (lmdb/get-val conn [:flag]))))

    (testing "delete removes key"
      (lmdb/put conn [:to-delete] true)
      (lmdb/delete conn [:to-delete])
      (is (nil? (lmdb/get-val conn [:to-delete]))))

    (.close (:env conn))))

;; ---- Prefix search ----

(deftest prefix-search-test
  (let [conn (lmdb/open-db (temp-dir) "prefix-test")]
    (lmdb/put conn [:circuit :op-1 1] true)
    (lmdb/put conn [:circuit :op-1 2] false)
    (lmdb/put conn [:circuit :op-2 1] true)
    (lmdb/put conn [:other  :op-1 1] true)

    (testing "prefix on first element returns all under that namespace"
      (is (= 3 (count (lmdb/prefix-search conn [:circuit])))))

    (testing "prefix on first two elements"
      (is (= 2 (count (lmdb/prefix-search conn [:circuit :op-1])))))

    (testing "full key returns exactly one result"
      (is (= 1 (count (lmdb/prefix-search conn [:circuit :op-1 1])))))

    (testing "decoded keys and values are correct"
      (let [results (into {} (lmdb/prefix-search conn [:circuit :op-1]))]
        (is (= true  (results [:circuit :op-1 1])))
        (is (= false (results [:circuit :op-1 2])))))

    (testing "non-matching prefix returns empty"
      (is (= [] (lmdb/prefix-search conn [:missing]))))

    (testing "prefix :circuit does not match :circuit-x keys"
      (lmdb/put conn [:circuit-x :op-1 1] true)
      (is (= 3 (count (lmdb/prefix-search conn [:circuit])))
          "[:circuit] prefix must not bleed into [:circuit-x]"))

    (.close (:env conn))))

(deftest prefix-search-mixed-types
  (let [conn (lmdb/open-db (temp-dir) "mixed-prefix")]
    ;; Simulate wizard circuit state: [op-sym entity-id attr wt] → boolean
    (lmdb/put conn ['datom-filter 1 :attr 10] true)
    (lmdb/put conn ['datom-filter 2 :attr 10] true)
    (lmdb/put conn ['datom-filter 1 :attr 20] false)
    (lmdb/put conn ['integrate-op 1]           true)

    (testing "prefix by op symbol"
      (is (= 3 (count (lmdb/prefix-search conn ['datom-filter])))))

    (testing "prefix by op symbol + entity id"
      (is (= 2 (count (lmdb/prefix-search conn ['datom-filter 1])))))

    (testing "different op symbol does not bleed"
      (is (= 1 (count (lmdb/prefix-search conn ['integrate-op])))))

    (.close (:env conn))))
