(ns wizard.circuit.codegen
  "Ahead-of-time generation of circuit definitions to per-circuit .clj/.cljs files.

  Given a wizard config, ensures the workspace directory layout exists, writes
  one EDN definition per circuit into <workspace>/definitions/, and emits one
  source file per circuit under a namespace prefix. Each generated ns defines
  `circuit-fn` (the fully macroexpanded output of
  `wizard.circuit-impl-inline/reify-circuit`).

  For circuits `{:my-view ..., :other-view ...}` and prefix `my-app.circuits`:
    - ns `my-app.circuits.my-view`,    file `src/my_app/circuits/my_view.<ext>`
    - ns `my-app.circuits.other-view`, file `src/my_app/circuits/other_view.<ext>`

  Durability:
    Circuit graphs are not deterministic (gensym node ids), so once a circuit
    has been generated its EDN and child .clj/.cljs are treated as durable.
    Regeneration only happens when:
      - the child file is missing, or
      - the config's `[query rules]` differs from what was persisted when the
        circuit was last built (tracked in `<id>.query.edn`), or
      - the EDN was hand-edited (mtime newer than child), or
      - `:force? true` is passed to `generate!`.

  Config keys:
    :wizard/circuit-ns-prefix  Symbol. Prefix under which each circuit's ns is
                               generated. Files placed under src/<prefix-path>/.
                               If absent, defaults to `wizard.circuits.generated`
                               and files are written under
                               <workspace-dir>/<prefix-path>/ (mirroring ns path).
    :wizard/target             :clj (default) or :cljs. Chooses file extension
                               and toggles cljs.core routing for user query fns."
  (:require
   [caudex.circuit :as c]
   [caudex.utils :as c.utils]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [wizard.circuit-impl-inline :as impl]
   [wizard.config :as config])
  (:import
   (java.nio.file
    Files
    LinkOption
    Path
    Paths)
   (java.nio.file.attribute
    PosixFilePermissions)))


(def ^:private default-ns-prefix 'wizard.circuits.generated)


(def ^:private runtime-requires
  '[[wizard.circuit.state]
    [wizard.zset]
    [wizard.circuit-impl-inline-fns]])


(defn- ensure-dirs!
  [^Path main-path]
  (let [edn-path       (.resolve main-path "definitions")
        circuits-path  (.resolve main-path "circuits")
        data-path      (.resolve main-path "data")
        perms          (into-array
                        [(PosixFilePermissions/asFileAttribute
                          (PosixFilePermissions/fromString "rwxr-xr--"))])]
    (doseq [^Path p [main-path edn-path circuits-path data-path]]
      (Files/createDirectories p perms))
    {:main main-path
     :edn  edn-path
     :data data-path}))


(defn- workspace-path
  ^Path [workspace-dir]
  (Paths/get (java.net.URI/create (str "file://" workspace-dir))))


(defn- circuit-edn-path
  ^Path [edn-dir circuit-id]
  (.resolve ^Path edn-dir (str (name circuit-id) ".edn")))


(defn- circuit-query-path
  ^Path [edn-dir circuit-id]
  (.resolve ^Path edn-dir (str (name circuit-id) ".query.edn")))


(defn- path->str ^String [^Path p]
  (-> p (.toAbsolutePath) (.toString)))


(defn- path-exists? [^Path p]
  (Files/exists p (into-array LinkOption [])))


(defn- ensure-circuit-edn!
  "Ensures the circuit's graph EDN and source-query sidecar reflect the config.
  Returns {:circuit <graph> :rebuilt? <bool>}.

  Rebuilds (from `c/build-circuit`) if any of:
    - no EDN exists yet (bootstrap)
    - persisted query differs from current config's [query rules]
    - force? is true
  Otherwise reads the existing EDN and returns it as-is (durable)."
  [edn-dir circuit-id circuit-conf & {:keys [force?]}]
  (let [{:wizard.circuit/keys [query rules]} circuit-conf
        edn-p          (circuit-edn-path edn-dir circuit-id)
        qry-p          (circuit-query-path edn-dir circuit-id)
        edn-exists?    (path-exists? edn-p)
        prev-query     (when (path-exists? qry-p)
                         (edn/read-string (slurp (path->str qry-p))))
        curr-query     {:query query :rules (or rules [])}
        query-changed? (and prev-query (not= prev-query curr-query))
        rebuild?       (or force? (not edn-exists?) query-changed?)]
    (when query-changed?
      (binding [*out* *err*]
        (println (str "[codegen] WARNING: query for " circuit-id
                      " has changed; rebuilding circuit. "
                      "Persisted data at <workspace>/data/" (name circuit-id)
                      " may no longer match the new shape."))))
    (let [circuit (if rebuild?
                    (c/build-circuit query rules)
                    (c.utils/edn->circuit (edn/read-string (slurp (path->str edn-p)))))]
      (when rebuild?
        (spit (path->str edn-p) (pr-str (c.utils/circuit->edn circuit)))
        (spit (path->str qry-p) (pr-str curr-query)))
      {:circuit circuit :rebuilt? rebuild?})))


(defn- target
  [{:wizard/keys [target]}]
  (or target :clj))


(defn- ext-for
  [target]
  (case target :clj "clj" :cljs "cljs"))


(defn ns-prefix
  "Namespace prefix for generated circuits. Uses :wizard/circuit-ns-prefix from
  the config, or `wizard.circuits.generated` as a default."
  [conf]
  (or (:wizard/circuit-ns-prefix conf) default-ns-prefix))


(defn child-ns-sym
  "Full namespace symbol for the given circuit: `<prefix>.<circuit-id>`."
  [conf circuit-id]
  (symbol (str (ns-prefix conf) "." (name circuit-id))))


(defn- ns-path-segments
  [ns-sym]
  (-> (name ns-sym)
      (str/replace "-" "_")
      (str/split #"\.")))


(defn child-file
  "Filesystem path where the child ns file for `circuit-id` is written."
  ^java.io.File [conf circuit-id]
  (let [ext        (ext-for (target conf))
        child      (child-ns-sym conf circuit-id)
        segments   (ns-path-segments child)
        [dir file] [(butlast segments) (str (last segments) "." ext)]
        base       (if (:wizard/circuit-ns-prefix conf)
                     (io/file "src")
                     (io/file (:wizard/workspace-dir conf)))]
    (apply io/file base (concat dir [file]))))


(defn all-child-files
  "Vector of child file paths for every circuit in the config, in id order."
  [{:wizard/keys [circuits] :as conf}]
  (mapv #(child-file conf %) (keys (sort circuits))))


(defn- expand-circuit
  [circuit cljs?]
  (walk/macroexpand-all
   (list `impl/reify-circuit circuit cljs?)))


(defn- expand-atom-state
  [circuit]
  (walk/macroexpand-all
   (list 'wizard.circuit.state/mk-atom-state circuit)))


(defn- emit-child-source
  [child-ns fn-form state-form]
  (let [ns-form       (list 'ns child-ns
                            "AUTO-GENERATED by wizard.circuit.codegen. Do not edit by hand."
                            (list* :require runtime-requires))
        def-form      (list 'def 'circuit-fn fn-form)
        make-state-fn (list 'defn 'make-atom-state [] state-form)]
    (str (with-out-str (pprint/pprint ns-form))
         "\n"
         (with-out-str
           (binding [pprint/*print-right-margin* 120]
             (pprint/pprint def-form)))
         "\n"
         (with-out-str
           (binding [pprint/*print-right-margin* 120]
             (pprint/pprint make-state-fn))))))


(defn- child-fresh?
  "True if the child file exists AND is at least as new as its EDN, AND the
  ensure-circuit-edn! step didn't rebuild the circuit."
  [conf id circuit-rebuilt?]
  (let [out-file (child-file conf id)
        edn-file (io/file (:wizard/workspace-dir conf)
                          "definitions"
                          (str (name id) ".edn"))]
    (and (.exists out-file)
         (not circuit-rebuilt?)
         (or (not (.exists edn-file))
             (<= (.lastModified edn-file) (.lastModified out-file))))))


(defn generate!
  "Idempotent: ensures per-circuit EDN + query sidecar + child source file exist
  and are consistent. Only regenerates artifacts that are actually stale.
  Options:
    :force?  Rebuild everything unconditionally.

  Returns {:outputs [{:id :file :written?} ...] :target ... :circuit-ids [...]}
  so callers can log which files were touched."
  [{:wizard/keys [workspace-dir circuits] :as conf} & {:keys [force?]}]
  (config/ensure-config-valid conf)
  (let [tgt           (target conf)
        cljs?         (= :cljs tgt)
        {:keys [edn]} (ensure-dirs! (workspace-path workspace-dir))
        outputs       (into []
                            (for [[id circuit-conf] (sort circuits)]
                              (let [{:keys [circuit rebuilt?]}
                                    (ensure-circuit-edn! edn id circuit-conf :force? force?)
                                    out-file (child-file conf id)]
                                (if (child-fresh? conf id rebuilt?)
                                  {:id id :file out-file :written? false}
                                  (let [fn-form    (expand-circuit circuit cljs?)
                                        state-form (expand-atom-state circuit)
                                        child-ns   (child-ns-sym conf id)
                                        source     (emit-child-source child-ns fn-form state-form)]
                                    (io/make-parents out-file)
                                    (spit out-file source)
                                    {:id id :file out-file :written? true})))))]
    {:outputs     outputs
     :target      tgt
     :circuit-ids (mapv :id outputs)}))
