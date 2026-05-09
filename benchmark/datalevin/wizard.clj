(ns benchmark.datalevin.wizard
  (:require
   [datalevin.core :as d]
   [benchmark.datalevin.job :as job]
   [wizard.circuit-impl-inline :as impl]
   [wizard.rocksdb.circuit-state :as rocksdb.state]))



(defn test-bench [& _]
  (let [circuit (caudex.circuit/build-circuit
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
        circ (eval `(impl/reify-circuit ~circuit))
        db (d/db job/conn)
        c-state (rocksdb.state/rocksdb-state "/tmp/bench-test" circuit #_{:initializing? true})
        siz 1000
        start-t (System/nanoTime)]

    (loop [datoms (d/seek-datoms db :eav nil nil nil siz) datoms-processed 0]
      (let [batch-t (System/nanoTime)
            delta (circ c-state datoms)
            datoms-processed (+ datoms-processed (count datoms))
            remaining (d/seek-datoms db :eav (inc (:e (last datoms))) nil nil siz)]
        (prn "datoms processed" datoms-processed
             "batch took" (/ (double (- (System/nanoTime) batch-t)) 1e6) "ms"
             "time elapsed" (/ (double (- (System/nanoTime) start-t)) 1e6) "ms"
             "delta" delta)
        (if (seq remaining)
          (recur remaining datoms-processed)
          datoms-processed)))

    #_(loop [attrs attrs
             siz siz
             datoms-processed 0]
        (if-let [attr (first attrs)]
          (let [_ (prn "processing" attr)
                datoms (d/datoms db :ave attr)
                attr-start-t (System/nanoTime)
                {next-siz :siz datoms-processed :processed}
                (batch-process-datoms-adaptive circ c-state siz datoms datoms-processed)]
            (prn attr "finished took" (/ (double (- (System/nanoTime) attr-start-t)) 1e6))
            (System/gc)
            (recur (rest attrs) next-siz datoms-processed))
          (with-open [c-state (lmdb.state/lmdb-state "/tmp/bench-test" circuit {:initializing? true})]
            (prn "result" (st/get-view c-state) "total time elapsed" (/ (double (- (System/nanoTime) start-t)) 1e6)))))))

