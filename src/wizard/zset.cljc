(ns wizard.zset
  (:require
   [caudex.dbsp :as dbsp]
   [me.tonsky.persistent-sorted-set :as sset]))

(defn tx-data->zset [tx-data]
  (apply sset/sorted-set (mapv (fn [[e a v _tx add?]]
                                 [e a v add?])
                               tx-data)))

(defn- mk-comparator [indices]
  (let [cmp (fn [row-1 row-2]
              (loop [idx (first indices) indices (rest indices)]
                (let [val-1 (nth row-1 idx)
                      val-2 (nth row-2 idx)
                      r (if (or (= val-1 :*) (= val-2 :*))
                          0
                          (if (= (type val-1) (type val-2))
                            (compare val-1 val-2)
                            ;; TODO enforce same types in zset entries?
                            (compare (str val-1) (str val-2)))
                          #_(compare val-1 val-2))]
                  (if (and (= r 0) (seq indices))
                    (recur (first indices) (rest indices))
                    r))))]
    cmp
    ;; #?(:clj (comparator cmp)
    ;;    :cljs cmp)
    ))

(defn mk-op-zset [op]
  (sset/sorted-set-by
   (mk-comparator (-> op dbsp/-get-output-type dbsp/-to-vector count inc range))))

(defn- get-join-indices [integrated-vars other-vars]
  (into []
        (comp
         (map-indexed vector)
         (map (fn [[idx var]]
                (when (and (symbol? var)
                           (contains? (set other-vars) var))
                  idx)))
         (filter some?))
        integrated-vars))

(defn mk-zset-init-fn-for-join [integrated-output other-output]
  (let [join-indices (get-join-indices integrated-output other-output)
        unused-indices (remove
                        #(contains? (set join-indices) %)
                        (range (inc (count integrated-output))))]
    (if (seq join-indices)
      #(sset/sorted-set-by (mk-comparator (into join-indices unused-indices)))
      #(sset/sorted-set-by (mk-comparator (-> integrated-output count inc range))))))


(defn add-zsets [zset-1 zset-2]
  (reduce
   (fn [zset row]
     (let [tupl (subvec row 0 (dec (count row)))
           remove-row (conj tupl (not (last row)))
           remove? (if (and (set? zset)
                            (seq (sset/slice zset remove-row remove-row)))
                     true
                     (some #(when (= % remove-row) true) zset))]
       (if (true? remove?)
         (if (set? zset)
           (disj zset remove-row)
           (filterv #(not= remove-row %) zset))
         (conj zset row))))
   zset-2
   zset-1))
