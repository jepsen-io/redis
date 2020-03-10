(ns jepsen.redis.append
  "Tests for transactional list append."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.pprint :refer [pprint]]
            [elle.core :as elle]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [util :as util :refer [parse-long]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as append]
            [jepsen.redis [client :as rc]]
            [taoensso.carmine :as car :refer [wcar]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn apply-mop!
  "Executes a micro-operation against a Carmine connection. This gets used in
  two different ways. Executed directly, it returns a completed mop. In a txn
  context, it's executed for SIDE effects, which must be reconstituted later."
  [conn [f k v :as mop]]
  (case f
    :r      [f k (wcar conn (car/lrange k 0 -1))]
    :append (do (wcar conn (car/rpush k (str v)))
                mop)))

(defn parse-read
  "Turns reads of [:r :x ['1' '2'] into reads of [:r :x [1 2]]."
  [[f k v :as mop]]
  (try
    (case f
      :r [f k (mapv parse-long v)]
      :append mop)
    (catch ClassCastException e
      (throw+ {:type  :unexpected-read-type
               :key   k
               :value v}))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (rc/delay-exceptions 5
      (assoc this :conn (rc/open node))))

  (setup! [_ test])

  (invoke! [_ test op]
    (rc/with-exceptions op #{}
      (->> (if (< 1 (count (:value op)))
             ; We need a transaction
             (->> (:value op)
                  ; Perform micro-ops for side effects
                  (mapv (partial apply-mop! conn))
                  ; In a transaction
                  (rc/with-txn conn)
                  ; And zip results back into the original txn
                  (mapv (fn [[f k v] r]
                          [f k (case f
                                 :r      r
                                 :append v)])
                        (:value op)))

             ; Just execute the mop directly, without a txn
             (->> (:value op)
                  (mapv (partial apply-mop! conn))))

           ; Parse integer reads
           (mapv parse-read)
           ; Returning that completed txn as an OK op
           (assoc op :type :ok, :value))))

  (teardown! [_ test])

  (close! [this test]
    (rc/close! conn)))

(defn workload
  "A list append workload."
  [opts]
  (-> (append/test {:key-count          (:key-count opts 3)
                    :min-txn-length     1
                    :max-txn-length     (:max-txn-length opts 4)
                    :max-writes-per-key 256
                    :anomalies          [:G2 :G1 :dirty-update]
                    :additional-graphs  [elle/realtime-graph]})
      (assoc :client (Client. nil))
;      (update :checker #(checker/compose {:workload %
;                                          :timeline (timeline/html)}))
      ))
