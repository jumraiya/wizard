(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'net.clojars.jumraiya/wizard)
(def version "0.1.2")
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (let [basis (b/create-basis {:project "deps.edn" :aliases []})]
    (b/write-pom {:class-dir class-dir
                  :lib lib
                  :version version
                  :basis basis
                  :src-dirs ["src"]
                  :scm {:url "https://github.com/jumraiya/wizard"
                        :connection "scm:git:git://github.com/jumraiya/wizard.git"
                        :developerConnection "scm:git:ssh://git@github.com/jumraiya/wizard.git"}})
    (b/jar {:class-dir class-dir
            :jar-file jar-file
            :basis basis
            :lib lib
            :version version})
    ;; Copy pom.xml to root for convenience
    (b/copy-file {:src (str class-dir "/META-INF/maven/" (namespace lib) "/" (name lib) "/pom.xml")
                  :target "pom.xml"})))

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
