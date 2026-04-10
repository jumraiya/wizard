(ns wizard.lmdb.circuit-test
  (:require [clojure.test :refer [deftest is]]
            [matcher-combinators.test]
            [wizard.circuit-impl-inline :as impl]
            [caudex.circuit :as c]
            [wizard.circuit-test-cases :as test-cases]
            [matcher-combinators.matchers :as m]
            [wizard.lmdb.circuit-state :as c.state])
  (:import (java.nio.file Files)
           (java.io File)))
;(ns-unalias 'wizard.lmdb.circuit-test 'impl)

(defn- temp-dir ^File []
  (let [f (.toFile (Files/createTempDirectory "lmdb-circuit-test" (make-array java.nio.file.attribute.FileAttribute 0)))]
    (.deleteOnExit f)
    (.getAbsolutePath f)))

(defn- make-state [circuit]
  (c.state/lmdb-state (temp-dir) circuit))


(deftest run-cases
  (doseq [{:keys [query rules data case]} test-cases/test-cases]
    (println (str "Testing " case))
    (let [ccircuit (c/build-circuit query rules)
          circuit (eval `(impl/reify-circuit ~ccircuit))
          c-state (make-state ccircuit)]
      (reduce
       (fn [c-state {:keys [tx output]}]
         (let [res (circuit c-state tx)]
           (when output
             (is (= res output)))
           c-state))
       c-state
       data))))
