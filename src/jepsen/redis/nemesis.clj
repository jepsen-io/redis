(ns jepsen.redis.nemesis
  "Nemeses for Redis"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [db :as db]
                    [generator :as gen]
                    [nemesis :as n]
                    [net :as net]
                    [util :as util]]
            [jepsen.nemesis [time :as nt]
                            [combined :as nc]]
            [jepsen.redis [db :as rdb]]))

(defn member-nemesis
  "A nemesis for adding and removing nodes from the cluster. Options:

    :db     The database to grow and shrink.

  Leave operations can either be a node name, or a map of {:remove node :using
  node}, in which case we ask a specific node to remove the other."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (info "Current membership\n" (with-out-str (pprint (rdb/node-state test))))
      (assoc op :value
             (case (:f op)
               :hold   nil
               :join   [(:value op) (rdb/join!  (:db opts) test (:value op))]
               :leave  [(:value op) (rdb/leave! (:db opts) test (:value op))])))

    (teardown! [this test] this)

    n/Reflection
    (fs [this] [:join :leave :hold])))

(def min-cluster-size
  "How small can the cluster get?"
  1)

(defn join-leave-gen
  "Emits join and leave operations for a DB."
  [db test process]
  (let [members (set (rdb/members db test))
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

(defn island-generator
  "A generator which picks a primary, isolates it from all other nodes, then
  issues leave requests to the isolated primary asking every other node to
  leave the cluster. Options:

    :db   - The database to island."
  [opts]
  (let [db    (:db opts)
        queue (atom nil)] ; A queue of ops we're going to emit.
    (->> (reify gen/Generator
           (op [_ test process]
             (first
               (swap! queue
                      (fn [q]
                        (if (seq q)
                          ; Advance
                          (next q)
                          ; Empty, refill. We pick a primary and generate
                          ; a queue of ops for it.
                          (let [p (rand-nth (db/primaries db test))
                                others (remove #{p} (:nodes test))]
                            (info "New island target is" p)
                            ; First, partition
                            (concat
                              [{:type  :info
                                :f     :start-partition
                                :value (n/complete-grudge [[p] others])}]
                              ; Then, remove all other nodes
                              (map (fn [n] {:type   :info
                                            :f      :leave
                                            :value  {:remove  n
                                                     :using   p}})
                                   others)))))))))
         (gen/delay (:interval opts)))))

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

(defn package-for
  "Builds a combined package for the given options."
  [opts]
  (->> (nc/nemesis-packages opts)
       (concat [(member-package opts)])
       (remove nil?)
       nc/compose-packages))

(defn package
  "Takes CLI opts; constructs a nemesis and generators for Redis. Options:

    :interval   How long to wait between operations.
    :db         The database to add/remove nodes from.
    :faults     The set of faults. A special fault, :island, yields islanding
                faults."
  [opts]
  (info opts)
  ; An island fault requires both membership and partition packages.
  (let [nemesis-opts (if (some #{:island} (:faults opts))
               (update opts :faults conj :member :partition)
               opts)
        ; Build a package for the options we have
        nemesis-package   (package-for nemesis-opts)
        ; And also for the generator faults we were explicitly asked for
        gen-package       (package-for opts)
        ; If we're asked for islanding faults, we need a special generator for
        ; those
        gen (if (some #{:island} (:faults opts))
              (if (= #{:island} (set (:faults opts)))
                ; No other generators, it's just us. God this is such a hack.
                (island-generator opts)
                ; Other generators
                (gen/mix [(island-generator opts)
                          (:generator gen-package)])))
        ; Should do a final gen here too but I'm lazy and we don't use final
        ; gens yet.
        ]
    ; Now combine em
    {:generator       gen
     :final-generator (:final-generator gen-package)
     :nemesis         (:nemesis nemesis-package)
     :perf            (:perf nemesis-package)}))
