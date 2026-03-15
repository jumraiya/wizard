(ns wizard.lmdb-test
  (:require [clojure.test :refer [deftest is testing]]
            [wizard.lmdb :as lmdb])
  (:import (java.nio.file Files)
           (java.io File)))

(defn- temp-dir ^File []
  (.toFile (Files/createTempDirectory "lmdb-test" (make-array java.nio.file.attribute.FileAttribute 0))))

(deftest encode-decode-roundtrip
  (let [conn (lmdb/open-db (temp-dir) "test")]
    (testing "string key + map value"
      (lmdb/put conn "hello" {:a 1 :b [2 3]})
      (is (= {:a 1 :b [2 3]} (lmdb/get-val conn "hello"))))

    (testing "keyword key + string value"
      (lmdb/put conn :my/key "world")
      (is (= "world" (lmdb/get-val conn :my/key))))

    (testing "vector key + nested map value"
      (lmdb/put conn [:x 42] {:nested {:deep true} :nums #{1 2 3}})
      (is (= {:nested {:deep true} :nums #{1 2 3}} (lmdb/get-val conn [:x 42]))))

    (testing "missing key returns nil"
      (is (nil? (lmdb/get-val conn :does-not-exist))))

    (testing "overwrite value"
      (lmdb/put conn "counter" 1)
      (lmdb/put conn "counter" 2)
      (is (= 2 (lmdb/get-val conn "counter"))))

    (testing "delete removes key"
      (lmdb/put conn :to-delete "bye")
      (lmdb/delete conn :to-delete)
      (is (nil? (lmdb/get-val conn :to-delete))))

    (.close (:env conn))))

(deftest prefix-search-test
  (let [conn (lmdb/open-db (temp-dir) "prefix-test")]
    (lmdb/put conn "user/alice" {:role :admin})
    (lmdb/put conn "user/bob"   {:role :viewer})
    (lmdb/put conn "user/carol" {:role :editor})
    (lmdb/put conn "team/eng"   {:size 5})
    (lmdb/put conn [:my-circuit :op-323232 435 "asd" :sdds] true)
    (lmdb/put conn [:my-circuit :op-323232 435 "asd" :css] true)

    (testing "returns all keys matching prefix"
      (let [results (lmdb/prefix-search conn "user/")]
        (is (= 3 (count results)))
        (is (= (set (map first results)) #{"user/alice" "user/bob" "user/carol"}))))

    (testing "values are decoded correctly"
      (let [results (into {} (lmdb/prefix-search conn "user/"))]
        (is (= {:role :admin}  (results "user/alice")))
        (is (= {:role :viewer} (results "user/bob")))))

    (testing "non-matching prefix returns empty"
      (is (= [] (lmdb/prefix-search conn "org/"))))

    (testing "exact prefix does not bleed into other namespaces"
      (is (= 1 (count (lmdb/prefix-search conn "team/")))))

    (testing "tuples work"
      (is (= 2 (count (lmdb/prefix-search conn [:my-circuit :op-323232 435 "asd"])))))

    (.close (:env conn))))
