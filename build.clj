(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'net.clojars.jumraiya/wizard)
(def version "0.1.2")
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn- pom-template []
  [[:description "A Clojure(script) state management library built using datascript"]
   [:url "https://github.com/jumraiya/wizard"]
   [:licenses
    [:license
     [:name "MIT License"]
     [:url "https://opensource.org/licenses/MIT"]]]
   [:developers
    [:developer
     [:name "Jaideep Umraiya"]]]
   [:distributionManagement
    [:repository
     [:id "clojars"]
     [:url "https://repo.clojars.org/"]]]])

(defn jar [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (let [basis (b/create-basis {:project "deps.edn" :aliases []})
        opts {:class-dir class-dir
              :lib lib
              :version version
              :basis basis
              :src-dirs ["src"]
              :pom-data (pom-template)
              :jar-file  (format "target/%s-%s.jar" lib version)}]
    (b/write-pom (assoc opts :src-pom :none))
    (b/jar opts)
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
