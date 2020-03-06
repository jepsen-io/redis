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
            [taoensso.carmine :as car :refer [wcar]]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (rc/delay-exceptions 5
      (assoc this :conn (rc/open node))))

  (setup! [_ test])

  (invoke! [_ test op]
    (rc/with-exceptions op #{}
      (->> (:value op)
           ; Perform micro-ops for side effects
           (mapv (fn [[f k v]]
                   (case f
                     :r      (wcar conn (car/lrange k 0 -1))
                     :append (wcar conn (car/rpush k (str v))))
                   ; We force a small delay to widen the concurrency window
                   ; here. Shouldn't matter, but it does break follower
                   ; proxies.
                   (Thread/sleep 500)))
           ; In a transaction
           (rc/with-txn conn)
           ; And zip results back into the original txn
           (mapv (fn [[f k v] r]
                   [f k (case f
                          :r      (mapv parse-long r)
                          :append v)])
                 (:value op))
           ; Returning that completed txn as an OK op
           (assoc op :type :ok, :value))))

  (teardown! [_ test])

  (close! [this test]
    (rc/close! conn)))

(defn workload
  "A list append workload."
  [opts]
  (-> (append/test {:key-count          3
                    :min-txn-length     1
                    :max-txn-length     4
                    :max-writes-per-key 256
                    :anomalies          [:G2 :G1 :dirty-update]
                    :additional-graphs  [elle/realtime-graph]})
      (assoc :client (Client. nil))
;      (update :checker #(checker/compose {:workload %
;                                          :timeline (timeline/html)}))
      ))
