(ns wizard.benchmark.job
  (:require [clojure.string :as s]))

(def aka-name-base        1000000)
(def aka-title-base       2000000)
(def cast-info-base       100000000)
(def char-name-base       10000000)
(def comp-cast-type-base  0)
(def company-name-base    3000000)
(def company-type-base    10)
(def complete-cast-base   4000000)
(def info-type-base       1000)
(def keyword-base         5000000)
(def kind-type-base       20)
(def link-type-base       30)
(def movie-companies-base 20000000)
(def movie-info-base      30000000)
(def movie-info-idx-base  40000000)
(def movie-keyword-base   50000000)
(def movie-link-base      100000)
(def name-base            60000000)
(def person-info-base     70000000)
(def role-type-base       50)
(def title-base           80000000)

(defn- add-comp-cast-type [reader]
  (map (fn [[id content]]
         [(+ ^long comp-cast-type-base (Long/parseLong id))
          :comp-cast-type/kind content])
       (d/read-csv reader)))

(defn- add-company-type [reader]
  (map (fn [[id content]]
         [(+ ^long company-type-base (Long/parseLong id))
          :company-type/kind content])
       (d/read-csv reader)))

(defn- add-kind-type [reader]
  (map (fn [[id content]]
         [(+ ^long kind-type-base (Long/parseLong id))
          :kind-type/kind content])
       (d/read-csv reader)))

(defn- add-link-type [reader]
  (map (fn [[id content]]
         [(+ ^long link-type-base (Long/parseLong id))
          :link-type/link content])
       (d/read-csv reader)))

(defn- add-role-type [reader]
  (map (fn [[id content]]
         [(+ ^long role-type-base (Long/parseLong id))
          :role-type/role content])
       (d/read-csv reader)))

(defn- add-info-type [reader]
  (map (fn [[id content]]
         [(+ ^long info-type-base (Long/parseLong id))
          :info-type/info content])
       (d/read-csv reader)))

(defn- add-movie-link [reader]
  (sequence
   (comp
    (map (fn [[id movie linked-movie link-type]]
           (let [eid (+ ^long movie-link-base (Long/parseLong id))]
             [[eid :movie-link/movie (+ ^long title-base (Long/parseLong movie))]
              [eid :movie-link/linked-movie (+ ^long title-base (Long/parseLong linked-movie))]
              [eid :movie-link/link-type (+ ^long link-type-base (Long/parseLong link-type))]])))
    cat)
   (d/read-csv reader)))

(defn- add-aka-name [reader]
  (sequence
   (comp
    (map
     (fn [[id person name imdb-index name-pcode-cf
           name-pcode-nf surname-pcode]]
       (let [eid (+ ^long aka-name-base (Long/parseLong id))]
         (cond-> [[eid :aka-name/person (+ ^long name-base (Long/parseLong person))]
                  [eid :aka-name/name name]]
           (not (s/blank? imdb-index))
           (conj [eid :aka-name/imdb-index imdb-index])
           (not (s/blank? name-pcode-cf))
           (conj [eid :aka-name/name-pcode-cf name-pcode-cf])
           (not (s/blank? name-pcode-nf))
           (conj [eid :aka-name/name-pcode-nf name-pcode-nf])
           (not (s/blank? surname-pcode))
           (conj [eid :aka-name/surname-pcode surname-pcode])))))
    cat)
   (d/read-csv reader)))

(defn load-datoms []
  )


