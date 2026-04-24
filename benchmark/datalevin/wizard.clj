(ns benchmark.datalevin.wizard
  (:require
   [datalevin.core :as d]
   [datalevin.interface :as di]
   [benchmark.datalevin.job :as job]
   [wizard.circuit-impl-inline :as impl]
   [wizard.lmdb.circuit-state :as lmdb.state]
   [wizard.rocksdb.circuit-state :as rocksdb.state]
   [wizard.circuit.state :as st]
   [clj-async-profiler.core :as prof]
   [clojure.string :as s])
  (:import (java.nio.file Files)
           (java.io File)))

;; --- Query transformation ---

(defn- find-items
  "Returns the items between :find and :where in a query vector."
  [query]
  (let [q         (vec query)
        find-idx  (.indexOf q :find)
        where-idx (.indexOf q :where)]
    (subvec q (inc find-idx) where-idx)))

(defn- min-item? [x]
  (and (seq? x) (= 'min (first x))))

(defn- strip-min
  "Replaces (min ?x) with ?x in the :find clause."
  [query]
  (let [q         (vec query)
        find-idx  (.indexOf q :find)
        where-idx (.indexOf q :where)
        stripped  (mapv #(if (min-item? %) (second %) %) (subvec q (inc find-idx) where-idx))]
    (into [:find] (concat stripped [:where] (subvec q (inc where-idx))))))

(defn- min-compare
  "min over a collection using compare (works for strings, numbers, etc.)."
  [coll]
  (reduce #(if (neg? (compare %1 %2)) %1 %2) coll))

(defn- apply-min
  "Applies (min ...) aggregation to a wizard result set.
   min-mask is a boolean vector; true at positions that had (min ...).
   Returns a set with one aggregated tuple, or nil if results is empty."
  [min-mask results]
  (when (seq results)
    #{(mapv (fn [i min?]
              (let [vals (map #(nth % i) results)]
                (if min? (min-compare vals) (first vals))))
            (range (count min-mask))
            min-mask)}))

;; --- Circuit building ---

(defn- build-inline-circuit
  "Builds a wizard inline circuit from a (already stripped) query using eval."
  [query]
  (eval `(impl/query->circuit '~query)))

;; --- Filter and collect queries ---

(defn- has-or? [query]
  (s/includes? (str query) "(or "))

(def ^:private query-entries
  "Sorted seq of [name-str query] for all non-or queries in job ns."
  (->> (ns-publics 'benchmark.datalevin.job)
       (filter #(s/starts-with? (name (key %)) "q-"))
       (sort-by (comp name key))
       (remove (fn [[_ v]] (has-or? (var-get v))))
       (map (fn [[k v]] [(name k) (var-get v)]))))

;; Build all circuits once at load time. Queries using unsupported predicates
;; (like, not-like, in) will throw and be stored as nil with an error message.
(def ^:private circuits
  (do
    (println "Building wizard circuits...")
    (into {}
          (map (fn [[qname query]]
                 (let [stripped (strip-min query)]
                   [qname (try
                            [::ok (build-inline-circuit stripped)]
                            (catch Exception e
                              [::err (.getMessage e)]))])))
          query-entries)))

;; --- Datom loading ---

;; Load all datalevin datoms into memory once for wizard processing.
;; NOTE: For large JOB datasets this may require significant heap space.
(def ^:private *all-datoms
  (delay
    (println "Loading all datoms from datalevin...")
    (d/datoms (d/db job/conn) :eav)))

;; --- Circuit execution helpers ---

(defn- new-state [] (st/->AtomCircuitState (atom {})))

(defn- apply-delta [view delta]
  (reduce
   (fn [v row]
     (if (last row)
       (conj v (vec (butlast row)))
       (disj v (vec (butlast row)))))
   view
   delta))

;; --- Benchmark runners ---

(defn- run-wizard [qname query]
  (let [[status val] (get circuits qname)]
    (case status
      ::err {:error val}
      ::ok  (let [min-mask (mapv min-item? (find-items query))
                  circuit  val
                  state    (new-state)
                  t0       (System/nanoTime)]
              (loop [datoms @*all-datoms res nil]
                (let [delta (circuit state (take 1000 datoms))
                      view (apply-delta #{} delta)
                      res (apply-min min-mask view)]
                  (if (seq datoms)
                    (recur (drop 1000 datoms) res)
                    {:ms (/ (double (- (System/nanoTime) t0)) 1e6) :result res}))))
      {:error "no circuit"})))

(defn- run-datalevin [query]
  (let [t0     (System/nanoTime)
        result (d/q query (d/db job/conn))
        ms     (/ (double (- (System/nanoTime) t0)) 1e6)]
    {:ms ms :result result}))

;; --- Output formatting ---

(defn- rpad [s n] (format (str "%-" n "s") (str s)))
(defn- lpad [s n] (format (str "%" n "s") (str s)))
(defn- fmt-ms [ms] (if ms (format "%8.1f ms" ms) (lpad "n/a" 11)))

(defn -main [& _]
  (println "\nWizard vs Datalevin — JOB benchmark (queries without 'or')")
  (println (rpad "query" 10)
           (lpad "wizard" 11)
           "  "
           (lpad "datalevin" 11)
           "  result")
  (println (apply str (repeat 60 "-")))
  (doseq [[qname query] query-entries]
    (let [wiz (run-wizard qname query)
          dl  (run-datalevin query)
          check (cond
                  (:error wiz)                   (str "SKIP: " (:error wiz))
                  (= (:result wiz) (:result dl)) "OK"
                  :else
                  (str "MISMATCH wizard=" (count (:result wiz))
                       " dl=" (count (:result dl))))]
      (println (rpad qname 10)
               (fmt-ms (:ms wiz))
               "  "
               (fmt-ms (:ms dl))
               "  " check)))
  (d/close job/conn)
  (shutdown-agents))

(defn batch-process-datoms [circ c-state siz datoms datoms-processed]
  (loop [datoms datoms datoms-processed datoms-processed]
    (let [attr-batch-t (System/nanoTime)
          data (take siz datoms)
          _delta (circ c-state data)
          datoms-processed (+ datoms-processed (count data))
          remaining (drop siz datoms)]
      (prn "datoms processed" datoms-processed  "batch took" (/ (double (- (System/nanoTime) attr-batch-t)) 1e6) "ms")
      (if (seq remaining)
        (recur remaining datoms-processed)
        datoms-processed))))


(defn test-bench [& _]
  (let [circuit (caudex.utils/edn->circuit
                 (clojure.edn/read-string
                  (slurp "/Users/jumraiya/projects/wizard/benchmark/datalevin/circuits/datalevin-join-benchmark.edn")))
        circ (eval `(impl/reify-circuit ~circuit))
        #_(caudex.circuit/build-circuit
           '[:find ?cn.name ?mi-idx.info ?t.title
             :where
             [?cn :company-name/country-code "[us]"]
             [?cn :company-name/name ?cn.name]
             [?ct :company-type/kind "production companies"]
             [?it1 :info-type/info "rating"]
             [?it2 :info-type/info "release dates"]
             [?kt :kind-type/kind "movie"]
             [?t :title/title ?t.title]
             [(not= ?t.title "")]
             (or-join [?t.title]
                      [(clojure.string/includes? ?t.title "Champion")]
                      [(clojure.string/includes? ?t.title "Loser")])
             [?mi :movie-info/movie ?t]
             [?mi :movie-info/info-type ?it2]
             [?t :title/kind ?kt]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mc :movie-companies/company-type ?ct]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mi-idx :movie-info-idx/info-type ?it1]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]])
        ;; ccirc (eval `(impl/reify-circuit ~circuit))
        db (d/db job/conn)
        attrs (->>
               [:movie-companies/company
                :kind-type/kind
                :title/title
                :movie-companies/movie
                :movie-info-idx/info-type
                :movie-info-idx/movie
                :title/kind
                :movie-info-idx/info
                :company-name/country-code
                :info-type/info
                :company-name/name
                :movie-info/info-type
                :company-type/kind
                :movie-companies/company-type
                :movie-info/movie]
               (sort-by
                #(d/count-datoms db nil % nil))
               reverse)
        ;; c-state (rocksdb.state/rocksdb-state "/tmp/bench-test" circuit {:initializing? true})
        siz 1000000
        start-t (System/nanoTime)]
    ;; prof/profile
    ;; {:event :wall}
    #_(loop [datoms (d/seek-datoms db :eav nil nil nil siz) datoms-processed 0]
      (let [batch-t (System/nanoTime)
            _delta (with-open [c-state (lmdb.state/lmdb-state "/tmp/bench-test" circuit {:initializing? true})]
                     (circ c-state datoms))
            datoms-processed (+ datoms-processed (count datoms))
            remaining (d/seek-datoms db :eav (inc (:e (last datoms))) nil nil siz)]
        (prn "datoms processed" datoms-processed
             "batch took" (/ (double (- (System/nanoTime) batch-t)) 1e6) "ms"
             "time elapsed" (/ (double (- (System/nanoTime) start-t)) 1e6) "ms")
        (if (seq remaining)
          (recur remaining datoms-processed)
          datoms-processed)))

    (loop [attrs attrs
             datoms-processed 0]
        (if-let [attr (first attrs)]
          (let [_ (prn "processing" attr)
                datoms (d/datoms db :ave attr)
                attr-start-t (System/nanoTime)
                datoms-processed (with-open [c-state (lmdb.state/lmdb-state "/tmp/bench-test" circuit {:initializing? true})]
                                   (batch-process-datoms circ c-state siz datoms datoms-processed))]
            (prn attr "finished took" (/ (double (- (System/nanoTime) attr-start-t)) 1e6))
            (recur (rest attrs) datoms-processed))
          (with-open [c-state (lmdb.state/lmdb-state "/tmp/bench-test" circuit {:initializing? true})]
            (prn "result" (st/get-view c-state) "total time elapsed" (/ (double (- (System/nanoTime) start-t)) 1e6)))))))


(comment
  ;; Run individual queries
  (run-wizard "q-2a" job/q-2a)
  (run-datalevin job/q-2a)

  (def circuit (caudex.utils/edn->circuit (clojure.edn/read-string (slurp "/Users/jumraiya/projects/wizard/benchmark/datalevin/circuits/datalevin-join-benchmark.edn"))))

  (def circ (eval `(impl/reify-circuit ~circuit)))

  (def db (d/db job/conn))
  (take 2 (d/seek-datoms db :eav nil nil nil 2))
  (loop [datoms (d/datoms db :ave :movie-info/movie)]
    (let [data (take 10000000 datoms)
          datoms (drop 10000000 datoms)]
      (prn "datoms" (count data))
      (if (seq datoms)
        (recur datoms)
        (prn "done"))))

  (def c-state (lmdb.state/lmdb-state "/tmp/bench-test" circuit))
  
  (prn (st/get-view c-state))

  (prn (d/seek-datoms db :eav nil nil nil 10))


  (def res
    #{["Hollywood Pictures" "4.4" "Breakfast of Champions"] ["Columbia Pictures Corporation" "6.6" "Blackhawk: Fearless Champion of Freedom"] ["Losers Take All" "4.7" "Losers Take All"] ["Weed Road Pictures" "6.3" "The Losers"] ["Fanfare Films" "5.2" "The Losers"] ["TBN Films" "5.0" "Carman: The Champion"] ["Primitive Nerd" "4.7" "Losers Take All"] ["Grand Champion Film Production" "4.3" "Grand Champion"] ["Hippo Motion LLC" "8.5" "Born Loser"] ["Reliable Pictures Corporation (I)" "4.1" "Loser's End"] ["Warner Bros" "6.3" "The Losers"] ["Perception Media" "6.9" "Beautiful Losers"] ["Code Productions" "5.0" "Carman: The Champion"] ["DL Sites" "5.8" "Losers Lounge"] ["TapouT Films" "7.4" "Once I Was a Champion"] ["Sneak Preview Entertainment" "2.5" "Beautiful Loser"] ["Springboard Films" "4.7" "Losers Take All"] ["Bear Media, The" "6.7" "5 Time Champion"] ["DC Entertainment" "6.3" "The Losers"] ["Centron Corporation" "3.7" "The Good Loser"] ["Jolliff Digital Production" "5.8" "Losers Lounge"] ["Supreme Studios" "2.3" "Supreme Champion"] ["Essanay Film Manufacturing Company, The" "6.7" "The Champion"] ["Metro-Goldwyn-Mayer (MGM)" "6.3" "Army Champions"] ["Shadow Motion Pictures" "1.8" "Champion Road: Arena"] ["501audio" "6.0" "God Thinks You're a Loser"] ["Summit Entertainment" "4.4" "Breakfast of Champions"] ["American International Productions" "5.7" "The Born Losers"] ["Rogue Arts" "5.5" "Loser"] ["Dark Castle Entertainment" "6.3" "The Losers"] ["Ted Fox Entertainment" "2.3" "Supreme Champion"] ["Metro-Goldwyn-Mayer (MGM)" "7.1" "Decathlon Champion: The Story of Glenn Morris"] ["Panorama Entertainment" "3.3" "Champions Forever: The Latin Legends"] ["Coyote County Productions" "5.7" "Coyote County Loser"] ["Muskat Filmed Properties" "5.4" "Champions"] ["Sidetrack Films" "6.9" "Beautiful Losers"] ["Shark Films" "6.7" "5 Time Champion"] ["Monogram Pictures" "6.9" "Lucky Losers"] ["Vitagraph Company of America" "3.9" "Battle of Jeffries and Sharkey for Championship of the World"] ["Rope the Moon Producions" "4.3" "Grand Champion"] ["Orchard Place Productions" "8.5" "Serial Loser"] ["Tough Crowd Productions" "7.4" "Once I Was a Champion"] ["Blacklake Productions" "6.9" "Beautiful Losers"] ["J & J Co." "5.5" "Jeffries-Johnson World's Championship Boxing Contest, Held at Reno, Nevada, July 4, 1910"] ["Egami Media" "6.3" "Roy Jones, Jr.: Heart of a Champion"] ["Paramount Pictures" "6.6" "Death of a Champion"] ["Stanley Kramer Productions" "7.4" "Champion"] ["Tri-Foot Productions" "6.0" "Night & Day Losers"] ["The Film Emporium" "7.0" "Champion"] ["Screen Plays" "7.4" "Champion"] ["Gener8Xion Entertainment" "5.0" "Carman: The Champion"] ["Fanfare Films" "5.7" "The Born Losers"] ["South Austin Pictures LLC" "6.0" "God Thinks You're a Loser"] ["New Champions Inc." "3.3" "Champions Forever: The Latin Legends"] ["Otis Productions" "5.7" "The Born Losers"] ["Ultimate Action" "2.3" "Supreme Champion"] ["Picture Perfect Entertainment" "2.3" "Supreme Champion"]})

  (def expected-res
    #{["Hollywood Pictures" "4.4" "Breakfast of Champions"]
      ["Columbia Pictures Corporation"
       "6.6"
       "Blackhawk: Fearless Champion of Freedom"]
      ["Losers Take All" "4.7" "Losers Take All"]
      ["Weed Road Pictures" "6.3" "The Losers"]
      ["Fanfare Films" "5.2" "The Losers"]
      ["TBN Films" "5.0" "Carman: The Champion"]
      ["Primitive Nerd" "4.7" "Losers Take All"]
      ["Grand Champion Film Production" "4.3" "Grand Champion"]
      ["Hippo Motion LLC" "8.5" "Born Loser"]
      ["Reliable Pictures Corporation (I)" "4.1" "Loser's End"]
      ["Warner Bros" "6.3" "The Losers"]
      ["Perception Media" "6.9" "Beautiful Losers"]
      ["Code Productions" "5.0" "Carman: The Champion"]
      ["DL Sites" "5.8" "Losers Lounge"]
      ["TapouT Films" "7.4" "Once I Was a Champion"]
      ["Sneak Preview Entertainment" "2.5" "Beautiful Loser"]
      ["Springboard Films" "4.7" "Losers Take All"]
      ["Bear Media, The" "6.7" "5 Time Champion"]
      ["DC Entertainment" "6.3" "The Losers"]
      ["Centron Corporation" "3.7" "The Good Loser"]
      ["Jolliff Digital Production" "5.8" "Losers Lounge"]
      ["Supreme Studios" "2.3" "Supreme Champion"]
      ["Essanay Film Manufacturing Company, The" "6.7" "The Champion"]
      ["Metro-Goldwyn-Mayer (MGM)" "6.3" "Army Champions"]
      ["Shadow Motion Pictures" "1.8" "Champion Road: Arena"]
      ["501audio" "6.0" "God Thinks You're a Loser"]
      ["Summit Entertainment" "4.4" "Breakfast of Champions"]
      ["American International Productions" "5.7" "The Born Losers"]
      ["Rogue Arts" "5.5" "Loser"]
      ["Dark Castle Entertainment" "6.3" "The Losers"]
      ["Ted Fox Entertainment" "2.3" "Supreme Champion"]
      ["Metro-Goldwyn-Mayer (MGM)"
       "7.1"
       "Decathlon Champion: The Story of Glenn Morris"]
      ["Panorama Entertainment"
       "3.3"
       "Champions Forever: The Latin Legends"]
      ["Coyote County Productions" "5.7" "Coyote County Loser"]
      ["Muskat Filmed Properties" "5.4" "Champions"]
      ["Sidetrack Films" "6.9" "Beautiful Losers"]
      ["Shark Films" "6.7" "5 Time Champion"]
      ["Monogram Pictures" "6.9" "Lucky Losers"]
      ["Vitagraph Company of America"
       "3.9"
       "Battle of Jeffries and Sharkey for Championship of the World"]
      ["Rope the Moon Producions" "4.3" "Grand Champion"]
      ["Orchard Place Productions" "8.5" "Serial Loser"]
      ["Tough Crowd Productions" "7.4" "Once I Was a Champion"]
      ["Blacklake Productions" "6.9" "Beautiful Losers"]
      ["J & J Co."
       "5.5"
       "Jeffries-Johnson World's Championship Boxing Contest, Held at Reno, Nevada, July 4, 1910"]
      ["Egami Media" "6.3" "Roy Jones, Jr.: Heart of a Champion"]
      ["Paramount Pictures" "6.6" "Death of a Champion"]
      ["Stanley Kramer Productions" "7.4" "Champion"]
      ["Tri-Foot Productions" "6.0" "Night & Day Losers"]
      ["The Film Emporium" "7.0" "Champion"]
      ["Screen Plays" "7.4" "Champion"]
      ["Gener8Xion Entertainment" "5.0" "Carman: The Champion"]
      ["Fanfare Films" "5.7" "The Born Losers"]
      ["South Austin Pictures LLC" "6.0" "God Thinks You're a Loser"]
      ["New Champions Inc." "3.3" "Champions Forever: The Latin Legends"]
      ["Otis Productions" "5.7" "The Born Losers"]
      ["Ultimate Action" "2.3" "Supreme Champion"]
      ["Picture Perfect Entertainment" "2.3" "Supreme Champion"]})

  (def circ (wizard.circuit-impl-inline/query->circuit
             '[:find ?cn.name ?mi-idx.info ?t.title
               :where
               [?cn :company-name/country-code "[us]"]
               [?cn :company-name/name ?cn.name]
               [?ct :company-type/kind "production companies"]
               [?it1 :info-type/info "rating"]
               [?it2 :info-type/info "release dates"]
               [?kt :kind-type/kind "movie"]
               [?t :title/title ?t.title]
               [(not= ?t.title "")]
               (or-join [?t.title]
                        [(clojure.string/includes? ?t.title "Champion")]
                        [(clojure.string/includes? ?t.title "Loser")])
               [?mi :movie-info/movie ?t]
               [?mi :movie-info/info-type ?it2]
               [?t :title/kind ?kt]
               [?mc :movie-companies/movie ?t]
               [?mc :movie-companies/company ?cn]
               [?mc :movie-companies/company-type ?ct]
               [?mi-idx :movie-info-idx/movie ?t]
               [?mi-idx :movie-info-idx/info-type ?it1]
               [?mi-idx :movie-info-idx/info ?mi-idx.info]]))

  (-> (d/seek-datoms (d/db job/conn) :eav nil nil nil 100) last :e)
  (d/seek-datoms (d/db job/conn) :eav 136244346 nil nil 1)
  (d/count-datoms (d/db job/conn) nil nil nil)

  (def cnt (atom {:processed 0 :view nil
                  :c-state (c.state/lmdb-state
                            "/tmp/bench-test"
                            "benchmark"
                            ;true
                            )}))

  (d/pull (d/db job/conn) '[*] 3000003)
  (:processed @cnt)

  (prof/serve-ui 8080)

  (def datums (d/seek-datoms (d/db job/conn) :eav 20016592 nil nil 50000))

  (prof/profile
   {:event :wall}
   (let [t0 (System/nanoTime)
         c-state (:c-state @cnt)
         db (d/db job/conn)
         siz 10000]
     (loop [datoms (d/seek-datoms db :eav nil nil nil siz) res nil t 0]
       (let [delta (circ c-state datoms)
             res (apply-delta (or res #{}) delta)
             last-eid (-> datoms last :e)
             datoms (d/seek-datoms db :eav (inc last-eid) nil nil siz)]
         (swap! cnt (fn [v] (-> v
                                (update :processed + siz)
                                (assoc :view res)
                                (assoc :last-eid last-eid))))
         (if (and (seq datoms) (not (:stop @cnt)))
           (recur datoms res (inc t))
           {:ms (/ (double (- (System/nanoTime) t0)) 1e6) :result res})))))

  (def m
    (future
      (try
        (let [t0 (System/nanoTime)
              c-state (:c-state @cnt)
              db (d/db job/conn)
              siz 10000]
          (loop [datoms (d/seek-datoms db :eav nil nil nil siz) res nil t 0]
            (let [delta (circ c-state datoms)
                  res (apply-delta (or res #{}) delta)
                  last-eid (-> datoms last :e)
                  datoms (d/seek-datoms db :eav (inc last-eid) nil nil siz)]
              (swap! cnt (fn [v] (-> v
                                     (update :processed + siz)
                                     (assoc :view res)
                                     (assoc :last-eid last-eid))))
              (if (and (seq datoms) (not (:stop @cnt)))
                (recur datoms res (inc t))
                {:ms (/ (double (- (System/nanoTime) t0)) 1e6) :result res}))))
        (catch Exception ex
          ex)))))
