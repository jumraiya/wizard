(ns wizard.compile-test
  (:require [clojure.test :refer [deftest is testing]]
            [wizard.compile :as compile]
            [wizard.circuit.state :as c.state])
  (:import [java.io File]))


;; :steps is always a sequence of [tx-data expected] pairs.
;; Use nil for expected when the result of a step is not under test.
(def test-cases
  [{:name "simple-select"
    :circuit {:name "simple-select"
              :query '[:find ?a ?b
                       :where
                       [?a :attr-1 ?b]]}
    :steps [[[[1 :attr-1 2 123 true]
              [2 :attr-2 12 123 true]]
             #{[1 2 true]}]]}

   {:name "simple-join"
    :circuit {:name "simple-join"
              :query '[:find ?a ?b
                       :where
                       [?a :attr-1 ?b]
                       [?b :attr-2 12]]}
    :steps [[[[1 :attr-1 2 123 true]
              [2 :attr-2 12 123 true]
              [3 :attr-1 4 123 true]
              [4 :attr-2 10 123 true]]
             #{[1 2 true]}]
            [[[2 :attr-2 12 124 false]
              [4 :attr-2 10 124 false]
              [4 :attr-2 12 124 true]]
             #{[1 2 false]
               [3 4 true]}]]}

   {:name "nested-rules"
    :circuit {:name "nested-rules"
              :query '[:find ?a ?b
                       :in $ %
                       :where
                       [?a :attr 12]
                       (rule ?a ?b)]
              :rules '[[(rule ?p ?q)
                         [?p :attr-2 :a]
                         [?q :attr-3 ?p]
                         (nested-rule ?q)]
                        [(nested-rule ?r)
                         [(= ?r 10)]]]}
    :steps [[[[1 :attr 12 123 true]
              [1 :attr-2 :a 123 true]
              [10 :attr-3 1 123 true]
              [2 :attr 12 123 true]
              [2 :attr-2 :a 123 true]
              [11 :attr-3 2 123 true]]
             #{[1 10 true]}]]}])


(defn- tmp-dir []
  (doto (File/createTempFile "wizard-circuits-dir" "")
    (.delete)
    (.mkdirs)))


(defn- run-compiled-circuits [ns-sym circuit-entries]
  (doseq [{:keys [name steps]} circuit-entries
          :let [circuit (var-get (ns-resolve ns-sym (symbol name)))
                c-state (c.state/->AtomCircuitState (atom {}))]]
    (testing name
      (doseq [[tx expected] steps]
        (let [result (circuit c-state tx)]
          (when (some? expected)
            (is (= expected result))))))))


(defn- base-config [ns-sym output-path edn-dir]
  {:output-ns   (str ns-sym)
   :output-path output-path
   :target      :clj
   :edn-dir     edn-dir})


(deftest test-compile-produces-edn-and-source
  (let [edn-dir    (str (tmp-dir))
        tmp-src    (File/createTempFile "wizard-compile" ".clj")
        output-path (.getAbsolutePath tmp-src)
        ns-sym     'wizard.compile-test-basic]
    (try
      (compile/compile-circuits
       (assoc (base-config ns-sym output-path edn-dir)
              :circuits    (mapv :circuit test-cases)
              :overwrite?  false))
      ;; EDN files were written
      (doseq [{:keys [name]} (mapv :circuit test-cases)]
        (is (.exists (File. edn-dir (str name ".edn")))))
      ;; Compiled circuits work correctly
      (load-file output-path)
      (run-compiled-circuits ns-sym test-cases)
      (finally
        (.delete tmp-src)
        (doseq [f (.listFiles (File. edn-dir))] (.delete f))
        (.delete (File. edn-dir))
        (remove-ns ns-sym)))))


(deftest test-existing-edn-without-overwrite-throws
  (let [edn-dir    (str (tmp-dir))
        tmp-src    (File/createTempFile "wizard-compile" ".clj")
        output-path (.getAbsolutePath tmp-src)
        ns-sym     'wizard.compile-test-overwrite-guard
        circuit    (first (mapv :circuit test-cases))]
    (try
      (compile/compile-circuits
       (assoc (base-config ns-sym output-path edn-dir)
              :circuits   [circuit]
              :overwrite? false))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"EDN already exists"
           (compile/compile-circuits
            (assoc (base-config ns-sym output-path edn-dir)
                   :circuits   [circuit]
                   :overwrite? false))))
      (finally
        (.delete tmp-src)
        (doseq [f (.listFiles (File. edn-dir))] (.delete f))
        (.delete (File. edn-dir))
        (remove-ns ns-sym)))))


(deftest test-existing-edn-with-overwrite-succeeds
  (let [edn-dir    (str (tmp-dir))
        tmp-src    (File/createTempFile "wizard-compile" ".clj")
        output-path (.getAbsolutePath tmp-src)
        ns-sym     'wizard.compile-test-overwrite
        circuit    (first (mapv :circuit test-cases))]
    (try
      (compile/compile-circuits
       (assoc (base-config ns-sym output-path edn-dir)
              :circuits   [circuit]
              :overwrite? false))
      (is (nil? (compile/compile-circuits
                 (assoc (base-config ns-sym output-path edn-dir)
                        :circuits   [circuit]
                        :overwrite? true))))
      (finally
        (.delete tmp-src)
        (doseq [f (.listFiles (File. edn-dir))] (.delete f))
        (.delete (File. edn-dir))
        (remove-ns ns-sym)))))


(deftest test-recompile-uses-existing-edn
  (let [edn-dir    (str (tmp-dir))
        tmp-src    (File/createTempFile "wizard-compile" ".clj")
        output-path (.getAbsolutePath tmp-src)
        ns-sym     'wizard.compile-test-recompile
        circuit    (first (mapv :circuit test-cases))]
    (try
      (compile/compile-circuits
       (assoc (base-config ns-sym output-path edn-dir)
              :circuits   [circuit]
              :overwrite? false))
      (let [edn-f     (File. edn-dir (str (:name circuit) ".edn"))
            edn-mtime (.lastModified edn-f)]
        (compile/compile-circuits
         (assoc (base-config ns-sym output-path edn-dir)
                :circuits    [circuit]
                :recompile?  true))
        (is (= edn-mtime (.lastModified edn-f)))
        (load-file output-path)
        (run-compiled-circuits ns-sym (filter #(= (:name circuit) (:name %)) test-cases)))
      (finally
        (.delete tmp-src)
        (doseq [f (.listFiles (File. edn-dir))] (.delete f))
        (.delete (File. edn-dir))
        (remove-ns ns-sym)))))


(deftest test-recompile-throws-when-edn-missing
  (let [edn-dir    (str (tmp-dir))
        tmp-src    (File/createTempFile "wizard-compile" ".clj")
        output-path (.getAbsolutePath tmp-src)
        ns-sym     'wizard.compile-test-recompile-missing]
    (try
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"EDN not found"
           (compile/compile-circuits
            (assoc (base-config ns-sym output-path edn-dir)
                   :circuits    (mapv :circuit test-cases)
                   :recompile?  true))))
      (finally
        (.delete tmp-src)
        (.delete (File. edn-dir))
        (remove-ns ns-sym)))))


(deftest test-compile-subset-of-circuits
  (let [edn-dir    (str (tmp-dir))
        tmp-src    (File/createTempFile "wizard-compile" ".clj")
        output-path (.getAbsolutePath tmp-src)
        ns-sym     'wizard.compile-test-subset
        all        (mapv :circuit test-cases)
        selected   (filter #(#{"simple-select" "nested-rules"} (:name %)) all)]
    (try
      (compile/compile-circuits
       (assoc (base-config ns-sym output-path edn-dir)
              :circuits   selected
              :overwrite? false))
      (is (.exists (File. edn-dir "simple-select.edn")))
      (is (.exists (File. edn-dir "nested-rules.edn")))
      (is (not (.exists (File. edn-dir "simple-join.edn"))))
      (load-file output-path)
      (run-compiled-circuits ns-sym (filter #(#{"simple-select" "nested-rules"} (:name %))
                                            test-cases))
      (finally
        (.delete tmp-src)
        (doseq [f (.listFiles (File. edn-dir))] (.delete f))
        (.delete (File. edn-dir))
        (remove-ns ns-sym)))))


(deftest test-load-config-from-clj-file
  (let [edn-dir     (str (tmp-dir))
        tmp-src     (File/createTempFile "wizard-compile" ".clj")
        output-path (.getAbsolutePath tmp-src)
        ns-sym      'wizard.compile-test-clj-config
        cfg-ns      'wizard.compile-test-clj-config-input
        cfg-file    (File/createTempFile "wizard-cfg" ".clj")]
    (try
      ;; Write a .clj config file with a `config` var.
      ;; Queries must be quoted in source so symbols like ?a don't get resolved.
      (spit cfg-file
            (str "(ns " cfg-ns ")\n"
                 "(def config\n"
                 "  {:output-ns   \"" (str ns-sym) "\"\n"
                 "   :output-path \"" output-path "\"\n"
                 "   :target      :clj\n"
                 "   :edn-dir     \"" edn-dir "\"\n"
                 "   :circuits    [{:name  \"simple-select\"\n"
                 "                  :query '[:find ?a ?b\n"
                 "                           :where\n"
                 "                           [?a :attr-1 ?b]]}]})\n"))
      (let [loaded (compile/load-config (.getAbsolutePath cfg-file))]
        (is (= (str ns-sym) (:output-ns loaded)))
        (compile/compile-circuits (assoc loaded :overwrite? false))
        (load-file output-path)
        (run-compiled-circuits ns-sym (take 1 test-cases)))
      (finally
        (.delete cfg-file)
        (.delete tmp-src)
        (doseq [f (.listFiles (File. edn-dir))] (.delete f))
        (.delete (File. edn-dir))
        (remove-ns ns-sym)
        (remove-ns cfg-ns)))))
