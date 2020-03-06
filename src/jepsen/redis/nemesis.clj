(ns jepsen.redis.nemesis
  "Nemeses for Redis"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [generator :as gen]
                    [nemesis :as n]
                    [net :as net]
                    [util :as util]]
            [jepsen.nemesis [time :as nt]
                            [combined :as nc]]
            [jepsen.redis [db :as db]]))

(defn member-nemesis
  "A nemesis for adding and removing nodes from the cluster. Options:

    :db     The database to grow and shrink."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (info "Current membership\n" (with-out-str (pprint (db/node-state test))))
      (assoc op :value
             (case (:f op)
               :hold   nil
               :join   [(:value op) (db/join!  (:db opts) test (:value op))]
               :leave  [(:value op) (db/leave! (:db opts) test (:value op))])))

    (teardown! [this test] this)

    n/Reflection
    (fs [this] [:join :leave :hold])))

(def min-cluster-size
  "How small can the cluster get?"
  1)

(defn join-leave-gen
  "Emits join and leave operations for a DB."
  [db test process]
  (let [members (set (db/members db test))
        addable (remove members (:nodes test))]
    (cond ; We can add someone
          (and (seq addable) (< (rand) 0.5))
          {:type :info, :f :join, :value (rand-nth (vec addable))}

          ; We can remove someone
          (< min-cluster-size (count members))
          {:type :info, :f :leave, :value (rand-nth (vec members))}

          ; Huh, no options at all.
          true
          {:type :info, :f :hold, :value {:type :can't-change
                                          :members members
                                          :nodes (:nodes test)}})))

(defn member-generator
  "Generates join and leave operations. Options:

    :db         The DB to act on.
    :interval   How long to wait between operations."
  [opts]
  ; Feels like the generator should be smarter here, and take over choosing
  ; nodes.
  (->> (partial join-leave-gen (:db opts))
       (gen/delay (:interval opts))))

(defn member-package
  "A membership generator and nemesis. Options:

    :interval   How long to wait between operations.
    :db         The database to add/remove nodes from.
    :faults     The set of faults. Should include :member to activate this
                package."
  [opts]
  (when ((:faults opts) :member)
    {:nemesis   (member-nemesis opts)
     :generator (member-generator opts)
     :perf      #{{:name  "join"
                   :fs    [:join]
                   :color "#E9A0E6"}
                  {:name  "leave"
                   :fs    [:leave]
                   :color "#ACA0E9"}}}))

(defn package
  "Takes CLI opts; constructs a nemesis and generators for Redis. Options:

    :interval   How long to wait between operations.
    :db         The database to add/remove nodes from.
    :faults     The set of faults. Should include :member to activate this
                package."
  [opts]
  (info opts)
  (-> (nc/nemesis-packages opts)
      (concat [(member-package opts)])
      (->> (remove nil?))
      nc/compose-packages))
