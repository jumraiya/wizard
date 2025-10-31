(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'net.clojars.jumraiya/wizard)
(def version "0.1.1")
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (let [basis (b/create-basis {:project "deps.edn"})]
    (b/write-pom {:class-dir class-dir
                  :lib lib
                  :version version
                  :basis basis
                  :src-dirs ["src"]})
    (b/jar {:class-dir class-dir
            :jar-file jar-file
            :basis basis
            :lib lib
            :version version})))

(defn uberjar [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (let [basis (b/create-basis {:project "deps.edn"})]
    (b/compile-clj {:basis basis
                    :src-dirs ["src"]
                    :class-dir class-dir})
    (b/uber {:class-dir class-dir
             :uber-file (str "target/" (name lib) "-" version "-standalone.jar")
             :basis basis})))

(defn deploy-jar [_]
  "Build a jar with compiled classes - caudex is now a Maven dependency"
  (clean nil)
  (let [basis (b/create-basis {:project "deps.edn"})]
    ;; Compile wizard source code
    (b/compile-clj {:basis basis
                    :src-dirs ["src"]
                    :class-dir class-dir})
    ;; Create jar with compiled classes (caudex included via Maven)
    (b/jar {:class-dir class-dir
            :jar-file jar-file})))
