(ns wizard.circuit-impl-inline-fns
  (:require [wizard.zset :as z]
            [wizard.circuit.state]))

(defn integrate [state tx op-id op input-1 join-zset]
  (wizard.circuit.state/add state tx op-id
                            (persistent!
                             (reduce
                              (fn [ss row]
                                (let [cur (z/at ss (z/->ZSetVecEntry (:tuple row) :*))]
                                  (if (and cur (not= (:wt row) (:wt cur)))
                                    (disj! ss cur)
                                    (conj! ss row))))
                              (transient join-zset)
                              (wizard.circuit.state/getv state tx input-1)))))

(defn join [state tx other-id integrated-id replace-map key-len join-zset flipped?]
  (reduce
   (fn [output delta-row]
     (let [lookup-key (mapv #(if (contains? replace-map %)
                               (nth (:tuple delta-row) (get replace-map %))
                               :*)
                            (range key-len))]
       (into output
             (reduce
              (fn [new-rows join-row]
                (conj new-rows
                      (if flipped?
                        (z/join-entry delta-row join-row)
                        (z/join-entry join-row delta-row))))
              []
              (wizard.circuit.state/slice state tx integrated-id lookup-key)))))
   join-zset
   (wizard.circuit.state/getv state tx other-id)))
