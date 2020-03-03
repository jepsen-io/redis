(ns jepsen.redis.append
  "Tests for transactional list append."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.pprint :refer [pprint]]
            [jepsen [client :as client]
                    [generator :as gen]
                    [util :as util :refer [parse-long]]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.redis [client :as rc]]
            [taoensso.carmine :as car :refer [wcar]]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (rc/open node)))

  (setup! [_ test])

  (invoke! [_ test op]
    (rc/with-exceptions op #{}
      (->> (:value op)
           ; Perform micro-ops for side effects
           (mapv (fn [[f k v]]
                   (case f
                     :r      (wcar conn (car/lrange k 0 -1))
                     :append (wcar conn (car/rpush k (str v))))))
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
  (-> (append/test {})
      (assoc :client (Client. nil))))
