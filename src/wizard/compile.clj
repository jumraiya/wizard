(ns wizard.compile
  "CLI utility to compile queries/rules into inline circuit functions.

  The command always produces an EDN file per circuit (source of truth) and
  generates a Clojure source file where each circuit is compiled via
  (impl/read-from-file ...) at compile time.

  Usage:
    clj -M:compile <input.edn> [--overwrite] [--recompile] [--circuits name1,name2,...]

  Flags:
    --overwrite          Overwrite existing EDN files (error by default).
    --recompile          Skip EDN generation; regenerate source from existing EDN files.
                         Throws if any required EDN file is missing.
    --circuits n1,n2     Compile only the named circuits from the config.

  Add to deps.edn aliases:
    :compile {:main-opts [\"-m\" \"wizard.compile\"]}

  Input can be an EDN file or a Clojure file. For a Clojure file the namespace
  must define a var named `config` containing the same map as the EDN format:

    (ns my.circuits-config)
    (def config
      {:output-ns   \"my.app.circuits\"
       :output-path \"src/my/app/circuits.clj\"
       ...})

  EDN format:
    {:output-ns   \"my.app.circuits\"
     :output-path \"src/my/app/circuits.clj\"   ; .clj or .cljs
     :target      :clj                          ; :clj or :cljs
     :edn-dir     \"resources/circuits\"          ; relative to project root
     :circuits    [{:name  \"find-player\"
                    :query [:find ?a ?b :where [?a :attr-1 ?b]]}
                   {:name  \"rule-circuit\"
                    :query [:find ?a ?b :in $ % :where [?a :attr 12] (rule ?a ?b)]
                    :rules [[(rule ?p ?q) [?p :attr-2 :a] [?q :attr-3 ?p]]]}]}"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.walk :as w]
            [caudex.circuit :as c]
            [clojure.string :as str]
            [wizard.circuit-impl-inline :as impl]
            [caudex.utils :as utils]))


(defn- load-clj-config [path]
  (let [ns-sym (with-open [r (java.io.PushbackReader. (io/reader path))]
                 (second (read r)))]    ; (ns foo.bar ...) -> foo.bar
    (load-file path)
    (if-let [v (ns-resolve (find-ns ns-sym) 'config)]
      @v
      (throw (ex-info (str "No `config` var found in " ns-sym)
                      {:ns ns-sym :path path})))))


(defn load-config [path]
  (if (re-find #"\.clj[c|s]?$" path)
    (load-clj-config path)
    (edn/read-string (slurp path))))


(defn- edn-file ^java.io.File [edn-dir circuit-name]
  (io/file edn-dir (str (if (string? circuit-name) circuit-name (name circuit-name)) ".edn")))


(defn- write-edn-file! [edn-dir {:keys [name query rules]}]
  (let [f (edn-file edn-dir name)]
    (io/make-parents f)
    (spit f (pr-str (utils/circuit->edn (c/build-circuit query rules))))
    (println "Written EDN" (str f))))


(defn- circuit-def [circuit-name edn-dir target]
  (let [body (w/macroexpand-all `(impl/read-from-file ~(str (edn-file edn-dir circuit-name)) ~target))]
   `(~'def ~(symbol circuit-name)
           ~body)))


(defn generate-forms [{:keys [output-ns target edn-dir circuits]}]
  (let [ns-form `(~'ns ~(symbol output-ns))]
    (cons ns-form
          (mapv #(circuit-def (:name %) edn-dir target) circuits))))


(defn compile-circuits
  [{:keys [output-path edn-dir circuits overwrite? recompile?] :as config}]
  (when (nil? edn-dir)
    (throw (ex-info ":edn-dir is required" {})))
  (if recompile?
    ;; Recompile: require all EDN files to already exist
    (doseq [{:keys [name]} circuits]
      (let [f (edn-file edn-dir name)]
        (when-not (.exists f)
          (throw (ex-info (str "EDN not found: " f
                               " — run without --recompile to generate it.")
                          {:path (str f)})))))
    ;; Normal: check for accidental overwrites, then write EDN files
    (do
      (doseq [{:keys [name]} circuits]
        (let [f (edn-file edn-dir name)]
          (when (and (.exists f) (not overwrite?))
            (throw (ex-info (str "EDN already exists: " f
                                 " — pass --overwrite to replace it.")
                            {:path (str f)})))))
      (doseq [spec circuits]
        (write-edn-file! edn-dir spec))))
  ;; Generate and write source file
  (let [source (with-out-str
                 (doseq [form (generate-forms config)]
                   (pp/pprint form)
                   (println)))]
    (io/make-parents output-path)
    (spit output-path source)
    (println "Written" (count circuits) "circuit(s) to" output-path)))


(defn- parse-args [args]
  (loop [remaining args
         result    {}]
    (if (empty? remaining)
      result
      (case (first remaining)
        "--overwrite"  (recur (rest remaining)
                              (assoc result :overwrite? true))
        "--recompile"  (recur (rest remaining)
                              (assoc result :recompile? true))
        "--circuits"   (recur (drop 2 remaining)
                             (assoc result :only-circuits
                                    (set (clojure.string/split (second remaining) #","))))
        "--output-ns"   (recur (drop 2 remaining)
                             (assoc result :output-ns
                                    (second remaining)))
        "--output-path"   (recur (drop 2 remaining)
                                 (assoc result :output-path
                                        (second remaining)))
        "--edn-dir"   (recur (drop 2 remaining)
                             (assoc result :edn-dir
                                    (second remaining)))
        "--target"   (recur (drop 2 remaining)
                            (assoc result :target
                                   (second remaining)))
        
        (if (contains? result :input)
          (throw (ex-info (str "Unexpected argument: " (first remaining)) {}))
          (recur (rest remaining)
                 (assoc result :input (first remaining))))))))


(defn -main [& args]
  (let [{:keys [input overwrite? recompile? only-circuits output-ns output-path edn-dir target]
         :or   {overwrite? false recompile? false}} (parse-args args)]
    (when (nil? input)
      (println "Usage: clj -M:compile <input.edn> [--overwrite] [--recompile] [--circuits n1,n2]")
      (System/exit 1))
    (let [config   (load-config input)
          circuits (cond->> (:circuits config)
                     only-circuits (filter #(contains? only-circuits (:name %))))]
      (when (empty? circuits)
        (println "Error: no matching circuits found")
        (System/exit 1))
      (compile-circuits (cond-> (assoc config
                                       :circuits   circuits
                                       :overwrite? overwrite?
                                       :recompile? recompile?)
                          output-ns (assoc :output-ns output-ns)
                          output-path (assoc :output-path output-path)
                          edn-dir (assoc :edn-dir edn-dir)
                          target (assoc :target target)))))
  (shutdown-agents))
