(ns wizard.circuit.watch
  "Background watcher: rebuilds the generated circuit .cljc whenever the config
  file changes. Startable from the command line via the :watch-circuits alias:

    clojure -M:watch-circuits path/to/config.clj

  Uses java.nio.file.WatchService (built into the JVM) so no extra dependencies
  are required at the consumer's side.

  The config file is a .clj(c) source file that declares a `config` var with a
  valid wizard config map (see wizard.config/WizardConfig)."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [wizard.circuit.codegen :as codegen])
  (:import
   (java.nio.file
    FileSystems
    Path
    StandardWatchEventKinds
    WatchEvent
    WatchKey
    WatchService)))


(defn- read-ns-form
  "Read the first form from `config-file`; expected to be a (ns ...) declaration.
  Returns the namespace symbol."
  [config-file]
  (with-open [r (java.io.PushbackReader. (io/reader config-file))]
    (let [form (edn/read {:eof ::eof :readers *data-readers*} r)]
      (when-not (and (seq? form) (= 'ns (first form)))
        (throw (ex-info "Config file must start with (ns ...) declaration"
                        {:file config-file :first-form form})))
      (second form))))


(defn load-config
  "Loads a wizard config from a .clj(c) file. The file must declare a namespace
  and define a `config` var. Returns the config map."
  [config-file]
  (let [ns-sym (read-ns-form config-file)]
    (load-file (.getAbsolutePath (io/file config-file)))
    (let [v (ns-resolve ns-sym 'config)]
      (when-not v
        (throw (ex-info "Config file must define a `config` var"
                        {:file config-file :ns ns-sym})))
      @v)))


(defn compile-once!
  "Load config from `config-file` and run the generator once. Idempotent: only
  circuits whose query has changed or whose child file is missing get written.
  Returns the generator result map."
  [config-file]
  (let [conf    (load-config config-file)
        result  (codegen/generate! conf)
        written (filter :written? (:outputs result))]
    (if (seq written)
      (doseq [{:keys [id file]} written]
        (println (str "[watch] wrote " (.getPath ^java.io.File file)
                      " (:" (name id) ")")))
      (println (str "[watch] no changes (" (count (:circuit-ids result)) " circuits up-to-date)")))
    result))


(defn- watch-loop
  [^WatchService ws ^Path dir-path ^java.io.File config-file on-change]
  (let [target-name (.getName config-file)]
    (try
      (loop []
        (let [^WatchKey key (.take ws)]
          (doseq [^WatchEvent e (.pollEvents key)]
            (let [ctx  (.context e)]
              (when (and (instance? Path ctx)
                         (= target-name (str (.getFileName ^Path ctx))))
                (try
                  (on-change)
                  (catch Throwable t
                    (binding [*out* *err*]
                      (println "[watch] error:" (.getMessage t))))))))
          (when (.reset key)
            (recur))))
      (catch InterruptedException _)
      (catch java.nio.file.ClosedWatchServiceException _))))


(defn start!
  "Start watching `config-file`. Runs an initial compile, then re-compiles on
  every create/modify event for the file. Returns a map with `:service` (the
  WatchService) and `:thread` (the daemon watcher thread)."
  [config-file]
  (let [f (io/file config-file)]
    (when-not (.exists f)
      (throw (ex-info "Config file does not exist" {:file (str f)})))
    (compile-once! f)
    (let [abs-file  (.getAbsoluteFile f)
          dir       (.getParentFile abs-file)
          dir-path  (.toPath dir)
          ws        (.newWatchService (FileSystems/getDefault))
          _         (.register dir-path ws
                               (into-array [StandardWatchEventKinds/ENTRY_CREATE
                                            StandardWatchEventKinds/ENTRY_MODIFY]))
          on-change #(compile-once! abs-file)
          thread    (doto (Thread. ^Runnable
                                   (fn [] (watch-loop ws dir-path abs-file on-change))
                                   "wizard-circuit-watcher")
                      (.setDaemon true)
                      (.start))]
      (println (str "[watch] watching " (.getAbsolutePath abs-file)))
      {:service ws :thread thread})))


(defn stop!
  "Stop a watcher returned by `start!`."
  [{:keys [^WatchService service ^Thread thread]}]
  (when service (.close service))
  (when thread (.interrupt thread)))


(defn -main
  [& args]
  (let [config-file (first args)]
    (when-not config-file
      (binding [*out* *err*]
        (println "Usage: clojure -M:watch-circuits <path/to/config.clj>"))
      (System/exit 1))
    (start! config-file)
    @(promise)))


(defn watch
  "clojure -X entrypoint. Usage:
     clojure -X:watch-circuits :config '\"path/to/config.clj\"'"
  [{:keys [config]}]
  (when-not config
    (binding [*out* *err*]
      (println "Missing :config argument."))
    (System/exit 1))
  (start! (str config))
  @(promise))
