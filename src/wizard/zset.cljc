(ns wizard.zset
  (:require
   [caudex.dbsp :as dbsp]
   [wizard.lmdb :as lmdb]
   [org.replikativ.persistent-sorted-set :as sset]))


(defprotocol ZSet
  (slice [this prefix])
  (at [this k])
  (add [this zset-row]))

(defprotocol ZSetEntry
  (join-entry [this zset-row]))

(defrecord ZSetVecEntry [tuple wt]
  ;; Object
  ;; (equals [this other]
  ;;   (and (instance? other ZSetVecEntry)
  ;;        (= (:tuple this) (:tuple other))))
  ;; (hashCode [_]
  ;;   (hash tuple))
  ZSetEntry
  (join-entry [_this zset-row]
    (->ZSetVecEntry (into tuple (:tuple zset-row)) (and wt (:wt zset-row)))))


(extend-type org.replikativ.persistent_sorted_set.PersistentSortedSet
  ZSet
  (slice [this lookup]
    (sset/slice this lookup lookup))
  (at [this k]
    (when-let [res (slice this k)]
      (if (> (count res) 1)
        (throw (ex-info "more than one row found for key!" {:key k}))
        (first res))))
  (add [this zset-row]
    (let [cur (at this (conj (:tuple zset-row) :*))]
      (if (and cur (not= (:wt zset-row) (:wt cur)))
        (disj this cur)
        (conj this zset-row)))))

(defrecord LMDBZSet [conn]
  ZSet
  (slice [_this lookup]
    (lmdb/prefix-search conn lookup))
  (at [_this k]
    (lmdb/get-val conn k))
  (add [this zset-row]
    (let [cur (at this (:tuple zset-row))]
      (if (and cur (not= (:wt zset-row) (:wt cur)))
        (lmdb/delete conn (:tuple cur))
        (lmdb/put this (:tuple zset-row) (:wt zset-row))))))


(defn mk-comparator [indices]
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

(defn tx-data->zset [tx-data]
  (apply sset/sorted-set (mapv (fn [[e a v _tx add?]]
                                 [e a v add?])
                               tx-data)))



(defmacro compare-index [idx row-1-sym row-2-sym]
  `(let [val-1# (nth (:tuple ~row-1-sym) ~idx)
         val-2# (nth (:tuple ~row-2-sym) ~idx)
                                        ;val-1# (nth ~row-1-sym ~idx)
         ;val-2# (nth ~row-2-sym ~idx)
         ]
     (if (or (= val-1# :*) (= val-2# :*))
       0
       (if (= (type val-1#) (type val-2#))
         (compare val-1# val-2#)
         (compare (str val-1#) (str val-2#))))))

(defmacro compare-row [cur-idx remaining row-1-sym row-2-sym]
  `(let [res# (compare-index ~cur-idx ~row-1-sym ~row-2-sym)]
     (if (not= 0 res#)
       res#
       ~(if (empty? remaining)
          0
          `(compare-row ~(first remaining) ~(rest remaining) ~row-1-sym ~row-2-sym)))))

(defmacro gen-comparator [indices]
  (let [row-1-sym (gensym)
        row-2-sym (gensym)
        x-forms `(compare-row ~(first indices) ~(rest indices) ~row-1-sym ~row-2-sym)]
    `(fn [~row-1-sym ~row-2-sym]
       ~x-forms)))

(defmacro gen-op-zset [op]
  (let [op (if (record? op)
               op
               (eval op))]
      `(sset/sorted-set-by
        (gen-comparator ~(range (inc (count (dbsp/-to-vector (dbsp/-get-output-type op)))))))))

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


(defmacro gen-zset-init-body-for-join [integrated-output other-output]
  (let [join-indices (get-join-indices integrated-output other-output)
        unused-indices (remove
                        #(contains? (set join-indices) %)
                        (range (inc (count integrated-output))))]
    (if (seq join-indices)
      `(sset/sorted-set-by (gen-comparator ~(into join-indices unused-indices)))
      `(sset/sorted-set-by (gen-comparator ~(-> integrated-output count inc range))))))

(defn add-zset-row [zset row]
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

(defn add-zsets [zset-1 zset-2]
  (let [[iterate-over add-to] (if (< (count zset-1)
                                     (count zset-2))
                                [zset-1 zset-2]
                                [zset-2 zset-1])]
    (reduce add-zset-row add-to iterate-over)))
