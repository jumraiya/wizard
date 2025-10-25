(ns wizard.examples.adventure
  (:require [datascript.core :as ds]
            [wizard.core :as w]
            ;[adventure-data :as adv.data]
            ))


(defonce game-conn (atom nil))

(def initial-state
  {:locations
   [{:db/id -100
     :location/description "room with green walls, there is a painting of a guinea pig in a tuxedo on the north wall. There is a bed and a small safe by it. There are doors to the south and east."
     :db/ident :green-room}
    {:db/id -101
     :location/description "room with red walls, there is a table in the center of the room. A window with iron bars looks to east and has sunlight coming through. It reflects brightly on the door across the room on the western wall, which seems to have a handle made from an exotic metal. There is another door to the south."
     :db/ident :red-room}
    {:db/id -102
     :location/description "room with white walls, there are doors to the east and south. You can see daylight coming underneath the southern door."
     :db/ident :white-room}
    {:db/id -103
     :location/description "room with blue walls. There is a desk with some papers on it on the eastern wall."
     :db/ident :blue-room}
    {:db/id -104
     :location/description "garden, you see a pathway leading out to a road. You are free!"
     :db/ident :outside}]
   :objects
   [{:db/id -200
     :object/description "player"
     :object/detailed-description "player"
     :db/ident :player}
    {:db/id -201
     :object/description "bed"
     :object/detailed-description "The sheets are in disarray, there are bits of lettuce on it."
     :object/location :green-room
     :object/can-pickup? false}
    {:db/id -202
     :object/description "table"
     :object/detailed-description "It looks like an antique, it is well lit with sunlight."
     :object/location :red-room
     :object/can-pickup? false}
    {:db/id -203
     :object/description "safe"
     :object/detailed-description "It looks pretty old with a combination lock."
     :object/location :green-room
     :object/can-open? true
     :object/locked? true
     :db/ident :safe}
    {:db/id -204
     :object/description "desk"
     :object/detailed-description "It's covered with various books and papers. Your eye catches a page with a elaborate letterhead."
     :object/location :blue-room}
    {:db/id -205
     :object/description "page"
     :object/detailed-description "It reads 'We1come to my hous3 you l3ttuce thieving cret1n. Use y0ur wits to escape or die here, hahaha.'"
     :object/location :blue-room}
    {:db/id -206
     :object/description "handle"
     :object/detailed-description "It's ornate handle, it seems to be made of a light colored alloy which looks somewhat brittle."
     :object/location :red-room}
    {:db/id -208
     :object/description "bowl"
     :object/detailed-description "It's made of the finest glass. There is no sign of any impurity."
     :object/location :safe
     :object/can-pickup? false
     :db/ident :fish-bowl}]
   :exits
   [{:db/id -300
     :exit/location-1 :green-room
     :exit/location-1-wall :south
     :exit/location-2 :red-room
     :exit/location-2-wall :north
     :exit/locked? false}
    {:db/id -301
     :exit/location-1 :red-room
     :exit/location-1-wall :west
     :exit/location-2 :white-room
     :exit/location-2-wall :east
     :exit/locked? true}
    {:db/id -302
     :exit/location-1 :red-room
     :exit/location-1-wall :south
     :exit/location-2 :blue-room
     :exit/location-2-wall :north
     :exit/locked? false}
    {:db/id -303
     :exit/location-1 :white-room
     :exit/location-1-wall :south
     :exit/location-2 :outside
     :exit/location-2-wall :north
     :exit/locked? false}]})

(def schema
  {;; :location/description string
   ;; :object/description string
   ;; :object/detailed-description
   ;; :object/can-pickup?
   ;; :action/type
   ;; :action/args
   ;; :action/t
   :object/location {:db/valueType :db.type/ref}
   :object/unlocks {:db/valueType :db.type/ref}
   ;; exit/location-1-wall [:north :south ...]
   ;; exit/location-2-wall [:north :south ...]
   ;; exit/locked?
   :exit/location-1 {:db/valueType :db.type/ref}
   :exit/location-2 {:db/valueType :db.type/ref}
   ;; action/move-processed?
   ;; player-last-action/arg
   })

(def datomic-schema
  [{:db/ident :object/description
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :object/detailed-description
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :object/locked?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one}

   {:db/ident :object/can-open?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one}

   {:db/ident :object/can-pickup?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one}

   {:db/ident :object/opened?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one}

   {:db/ident :object/location
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/ident :object/combination
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :action/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}

   {:db/ident :action/arg
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :put-action/object
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :put-action/on-object
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   
   {:db/ident :location/description
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident :exit/locked?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one}
   
   {:db/ident :exit/location-1
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/ident :exit/location-1-wall
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}

   {:db/ident :exit/location-2
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/ident :exit/location-2-wall
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}])

(def datomic-init-data
  (update-vals
   initial-state
   (fn [ents]
     (mapv #(dissoc % :db/id) ents))))

(defn- init-game! []
  (reset! game-conn (ds/create-conn schema)) 
  (ds/transact! @game-conn (:locations initial-state)
                             ;adv.data/locations
                )
  (ds/transact! @game-conn (:objects initial-state)
                             ;adv.data/objects
                )
  (ds/transact! @game-conn (:exits initial-state)
                             ;adv.data/exits
                ))


(def rules
  '[[(get-player-loc ?loc)
     [?player :object/description "player"]
     [?player :object/location ?loc]]
    [(object-accessible? ?o ?arg ?det ?in-inventory)
     (or-join [?o ?arg ?det ?in-inventory]
              (and
               [?o :object/description ?arg]
               [?o :object/detailed-description ?det]
               [?player :object/description "player"]
               [?player :object/location ?player-loc]
               (not-join [?o]
                         [?o :object/description "player"])
               (or-join [?player-loc ?player ?o ?in-inventory]
                        (and [?o :object/location ?player]
                             [(ground true) ?in-inventory])
                        (and
                         [?o :object/location ?player-loc]
                         [(ground false) ?in-inventory])))
              (and
               (or-join [?arg]
                        [(= ?arg "player")]
                        (not-join [?arg]
                                  [_ :object/description ?arg])
                        (and
                         [?o :object/description ?arg]
                         [?p :object/description "player"]
                         [?p :object/location ?l]
                         (not-join [?l ?p ?o]
                                   (or-join [?l ?p ?o]
                                            [?o :object/location ?p]
                                            [?o :object/location ?l]))))
               [(ground :object-not-found) ?o]
               [(ground :no-description) ?det]
               [(ground false) ?in-inventory]))]
    [(new-action-added? [?action ?action-type ?action-arg ?handler-attr])
     [?action :action/type ?action-type]
     [?action :action/arg ?action-arg]
     (not-join [?action ?handler-attr]
               [?action ?handler-attr true])]
    [(get-else [?e ?attr ?val])
     (or-join [?e ?attr ?val]
              (and (not-join [?e ?attr]
                             [?e ?attr _])
                   [(ground false) ?val])
              [?e ?attr ?val])]])


(def state-change-handlers
  {::player-location
   {:query '[:find ?desc ?loc
             :where
             [?p :object/description "player"]
             [?p :object/location ?loc]
             [?loc :location/description ?desc]]
    :handler (fn [add _ret _view]
               (when-let [[desc] (first add)]
                 (println (str "You are in a " desc))))}
   ::move-action
   {:query '[:find ?a ?dest ?locked
             :in $ %
             :where
             [?p :object/description "player"]
             [?p :object/location ?loc]
             (new-action-added? ?a :move ?arg :action/move-processed?)
             [(keyword ?arg) ?wall]
             (or-join [?loc ?wall ?dest ?locked]
                      (and
                       [?exit :exit/location-1 ?loc]
                       [?exit :exit/location-1-wall ?wall]
                       [?exit :exit/location-2 ?dest]
                       [?exit :exit/locked? ?locked])
                      (and
                       [?exit :exit/location-2 ?loc]
                       [?exit :exit/location-2-wall ?wall]
                       [?exit :exit/location-1 ?dest]
                       [?exit :exit/locked? ?locked])
                      (and
                       (not-join [?loc ?wall]
                                 (or-join [?loc ?wall]
                                          (and
                                           [?e :exit/location-2 ?loc]
                                           [?e :exit/location-2-wall ?wall])
                                          (and
                                           [?e :exit/location-1 ?loc]
                                           [?e :exit/location-1-wall ?wall])))
                       [(ground :not-found) ?dest]
                       [(ground false) ?locked]))]
    :handler (fn [adds _ _]
               (when-let [[action-id new-loc locked?] (first adds)]
                 (let [tx (cond
                            (true? locked?) (do (println "The door is locked") [])
                            (not= :not-found new-loc)
                            [[:db/add :player :object/location new-loc]]
                            (= :not-found new-loc) (do (println "There is no door in that direction") [])
                            :else [])]
                   (conj tx [:db/add action-id :action/move-processed? true]))))}

   ::inspect-action
   {:query '[:find ?a ?o ?arg ?det
             :in $ %
             :where
             [?a :action/arg ?arg]
             [?a :action/type :inspect]
             (not-join [?a]
                       [?a :action/inspect-processed? true])
             (object-accessible? ?o ?arg ?det _)]
    :handler (fn [adds _ _]
               (when-let [[action-id object-id action-arg desc] (first adds)]
                 (if (= :object-not-found object-id)
                   (println (str "no " action-arg " found"))
                   (println desc))
                 [[:db/add action-id :action/inspect-processed? true]]))}

   ::pickup-action
   {:query '[:find ?a ?arg ?o ?can-pickup ?in-inventory
             :in $ %
             :where
             (new-action-added? ?a :pickup ?arg :action/pickup-processed?)
             (object-accessible? ?o ?arg ?det ?in-inventory)
             (get-else ?o :object/can-pickup? ?can-pickup)]
    :handler (fn [adds _ _]
               (when-let [[action-id desc object-id can-pickup? in-inventory?] (first adds)]
                 (into
                  [[:db/add action-id :action/pickup-processed? true]]
                  (cond
                    (= object-id :object-not-found)
                    (println (str "No " desc " found"))
                    in-inventory? (println (str "You already have " desc))
                    (not can-pickup?) (println (str "Cannot pickup " desc))
                    :else (do
                            (println (str "You picked up " desc))
                            [[:db/add object-id :object/location :player]])))))}
   ::bowl-on-table
   {:query '[:find ?o ?e
             :where
             [?o :object/description "bowl"]
             [?o :object/location ?l]
             [?l :object/description "table"]
             [?e :exit/location-1 ?el-1]
             [?el-1 :db/ident :red-room]
             [?e :exit/location-1-wall :west]]
    :handler (fn [adds _ _]
               (when-let [[object-id exit-id] (first adds)]
                 (println "The sun's rays pass through the bowl, converging on the door handle on the western door. It melts away.")
                 [[:db/add exit-id :exit/locked? false]
                  [:db/add object-id :object/location :red-room]]))}

   ::open-safe
   {:query '[:find ?o ?locked ?comb ?c ?desc
             :in $ %
             :where
             (new-action-added? ?a :open "safe" :action/open-processed?)
             (object-accessible? ?o "safe" ?det)
             [?o :object/locked? ?locked]
             [?c :object/location ?o]
             (get-else ?c :object/description ?desc)
             (get-else ?o :object/combination ?comb)]
    :handler (fn [adds _ _]
               (when-let [[object-id locked? combination c-id content]
                          (first adds)]
                 (let [[_ player-loc] (first (w/get-view ::player-location))]
                   (cond
                     (and (= combination "13310") locked?)
                     (do
                       (println (str "The safe opened! You see a " content " inside"))
                       [[:db/add object-id :object/locked? false]
                        [:db/add c-id :object/location player-loc]
                        [:db/add c-id :object/can-pickup? true]])
                     (not locked?) (println (cond-> "The safe is open."
                                              (string? content) (str " You see a " content " inside")))
                     (and (false? combination) locked?)
                     (println "You need a combination to open the safe. Try setting it by typing \"set combination xxxx\" and then \"open safe\"")
                     (and locked? (not= combination "13310"))
                     (println "The combination didn't work")))))}

   ::set-safe-combination
   {:query '[:find ?a ?code ?o
             :in $ %
             :where
             (object-accessible? ?o "safe" ?det _)
             (new-action-added? ?a :set-combination ?code :action/set-combination?)]
    :handler (fn [adds _ _]
               (when-let [[action-id code object-id] (first adds)]
                 (conj
                  [[:db/add action-id :action/set-combination? true]]
                  (if (= :object-not-found object-id)
                    (println "no safe in this room")
                    (do
                      (println (str "set combination to " code))
                      [:db/add object-id :object/combination code])))))}
   ::accessible-objects
   {:query '[:find ?desc ?in-inventory
             :in $ %
             :where
             [?o :object/description ?desc]
             [?o :object/detailed-description ?det]
             [?player :object/description "player"]
             [?player :object/location ?player-loc]
             (not-join [?o]
                       [?o :object/description "player"])
             (or-join [?player-loc ?player ?o ?in-inventory]
                      (and [?o :object/location ?player]
                           [(ground true) ?in-inventory])
                      (and
                       [?o :object/location ?player-loc]
                       [(ground false) ?in-inventory]))]
    :handler (fn [_ _ _])}

   ::accessible-exits
   {:query '[:find ?wall
             :in $ %
             :where
             (get-player-loc ?loc)
             (or-join [?loc ?wall]
                      (and
                       [?e :exit/location-1 ?loc]
                       [?e :exit/location-1-wall ?wall])
                      (and
                       [?e :exit/location-2 ?loc]
                       [?e :exit/location-2-wall ?wall]))]
    :handler (fn [_ _ _])}

   ::put-action
   {:query '[:find ?a ?desc ?on-desc ?o ?on-object
             :in $ %
             :where
             [?a :put-action/object ?desc]
             [?a :put-action/on-object ?on-desc]
             (object-accessible? ?o ?desc ?det ?i)
             (object-accessible? ?on-object ?on-desc ?det-2 ?i-2)]
    :handler (fn [adds _ _]
               (when-let [[action-id desc on-object-desc object-id on-object-id]
                          (first adds)]
                 (into
                  [[:db/retract action-id :put-action/object]
                   [:db/retract action-id :put-action/on-object]]
                  (cond
                    (= :object-not-found object-id)
                    (println (str "No " desc " found"))
                    (= :object-not-found on-object-id)
                    (println (str "No " on-object-desc " found"))
                    :else (do
                            (println (str "you put " desc " on " on-object-desc))
                            [[:db/add object-id :object/location on-object-id]])))))}

   ::open-action
   {:query '[:find ?a ?o ?arg ?can-open ?locked
             :in $ %
             :where
             (new-action-added? ?a :open ?arg :action/open-processed?)
             (object-accessible? ?o ?arg ?det _)
             (get-else ?o :object/can-open? ?can-open)
             (get-else ?o :object/locked? ?locked)]
    :handler (fn [adds _  _]
               (when-let [[action-id object-id desc can-open? locked?] (first adds)]
                 (let [tx (when (and can-open? (not locked?))
                            [[:db/add object-id :object/opened? true]])]
                   (cond
                     ;locked? (println (str desc " is locked"))
                     (not can-open?) (println (str "cannot open " desc))
                     :else nil)
                   (into
                    [[:db/add action-id :action/open-processed? true]]
                    tx))))}})

(defn do-action [action-str]
  (let [[action object & args] (clojure.string/split action-str #" ")
        transact-action #(w/transact
                          @game-conn
                          [{:db/id -1
                            :action/type %1
                            :action/arg %2}])]
    (case action
      "help" (println "You can use the following commands:\nmove [north|south|west|east]\ninspect [object]\npickup [object]")
      "move" (transact-action :move object)

      "inspect" (transact-action :inspect object)

      "open" (transact-action :open object)

      "put" (let [[_ on-object] args]
              (w/transact
               @game-conn
               [{:db/id -1
                 :put-action/object object
                 :put-action/on-object on-object}]))
      "look" (let [loc-desc (w/get-view ::player-location)
                   objects (w/get-view ::accessible-objects)
                   exits (mapv (comp name first) (w/get-view ::accessible-exits))]
               (println (str "You are in a " (ffirst loc-desc)))
               (println (str "You see a "
                             (clojure.string/join
                              ","
                              (into []
                                    (comp
                                     (filter #(false? (second %)))
                                     (map first))
                                    objects))))
               (when-let [objs (seq
                                (into []
                                      (comp
                                       (filter #(true? (second %)))
                                       (map first))
                                      objects))]
                 (println (str "You have a "
                               (clojure.string/join
                                ", "
                                objs))))
               (println (str "There are doors to the ") (clojure.string/join ", " exits)))

      "set" (if (= object "combination")
              (let [[code] args]
                (transact-action :set-combination code))
              (println "nothing happened"))

      "pickup" (transact-action :pickup object))
    nil))

(defn start-game []
  (init-game!)
  (doseq [[id {:keys [query handler]}] state-change-handlers]
    (w/add+subscribe-to-view @game-conn id handler query :rules rules))
  (w/transact @game-conn [[:db/add :player :object/location :green-room]])
  nil)

#?(:cljs
   (do
     (defn ^:export start-game-js []
       (start-game))
     
     (defn ^:export do-action-js [action-str]
       (do-action action-str))))

(comment
  (w/reset-all)
  (start-game)
  (do-action "move south")
  (do-action "move west")
  (do-action "move east")
  (do-action "inspect bed")
  (do-action "inspect handle")
  (do-action "inspect safe")
  (do-action "inspect desk")
  (do-action "inspect page")
  (do-action "move north")
  (do-action "inspect bed")
  (do-action "inspect table")

  (do-action "open safe")
  (do-action "pickup bowl")
  (do-action "look")
  (do-action "put bowl on table")
  (do-action "inspect bowl")
  (do-action "set combination 13311")
  (do-action "set combination 13310")
  )
