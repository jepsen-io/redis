(ns jepsen.eloqkv.append
  "Tests for transactional list append."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.pprint :refer [pprint]]
            [elle.core :as elle]
            [jepsen [checker :as checker]
             [client :as client]
             [generator :as gen]
             [util :as util :refer [parse-long]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :as append]
            [jepsen.eloqkv [client :as rc]]
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
  [conn [f k v :as mop]]
  ;; (info "parse-read mop:" mop)
  (try
    (case f
      :r [f k (mapv parse-long v)]
      :append mop)
    (catch ClassCastException e
      (throw+ {:type        :unexpected-read-type
               :key         k
               :value       v}))))
               ; We're getting QUEUED in response to non-MULTI operations; I'm
               ; trying to figure out why, and to get debugging info, I'm gonna
               ; log whatever happens from an EXEC here.
               ;:exec-result (wcar conn (car/exec))}))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (try
      (info "internal node(s): " (or (:internal-nodes test) []))
      (if (some #(= % node) (or (:internal-nodes test) []))
        ;; It's an internal node; just store nil for `conn` but still return a record
        (assoc this :conn nil)
        (rc/delay-exceptions 5
          (let [c (rc/open node)]
            (info "connect to node:" node)
            (assoc this :conn (rc/open node)))))

      (catch java.net.ConnectException e
        (warn "Caught exception during open connection on node" node
              ". Error message:" (.getMessage e))
        ;; (assoc this :conn nil)
      )

      ;; NEW: catch InterruptedException 
      (catch InterruptedException e
        (warn "Sleep was interrupted while opening node" node e)
        ;; (assoc this :conn nil)
      )

      ;; If you want to be extra safe, 
      ;; you could catch *all* exceptions and return the record:
      ;; (catch Throwable t
      ;;   (warn "Some other error in open!" t)
      ;;   (assoc this :conn nil))
      ))

  (setup! [_ test])

  (invoke! [_ test op]
    (if (nil? conn)
      (assoc op :type :fail, :error [:connect-error "Connection failed. This may be a storage or log service node."])
      (rc/with-exceptions op #{}
        (rc/with-conn test conn
          (->> (if (< 1 (count (:value op)))
               ; We need a transaction
                 (->> (:value op)
                    ; Perform micro-ops for side effects
                      (mapv (partial apply-mop! conn))
                    ; In a transaction
                      (rc/with-txn conn)
                    ; And zip results back into the original txn
                      (mapv (fn [[f k v] r]
                            ;; (info "mapv" f k v r)
                              [f k (case f
                                     :r      r
                                     :append v)])
                            (:value op)))

               ; Just execute the mop directly, without a txn
                 (->> (:value op)
                      (mapv (partial apply-mop! conn))))

             ; Parse integer reads
               (mapv (partial parse-read conn))
             ; Returning that completed txn as an OK op
               (assoc op :type :ok, :value))))))

  (teardown! [_ test])

  (close! [this test]
    (info "nil conn" (nil? conn))
    (if (nil? conn)
      nil
      (rc/close! conn))))

(defn workload
  "A list append workload."
  [opts]
  (-> (append/test {; Exponentially distributed, so half of the time it's gonna
                    ; be one key, 3/4 of ops will use one of 2 keys, 7/8 one of
                    ; 3 keys, etc.
                    :key-count          (:key-count opts 12)
                    :min-txn-length     1
                    :max-txn-length     (:max-txn-length opts 1)
                    :max-writes-per-key (:max-writes-per-key opts 16)
                    :anomalies         [:G1 :G2]
                    :additional-graphs [elle/realtime-graph]
                    :consistency-models [:repeatable-read]})
      (assoc :client (Client. nil))
      ;; (info "client:" (:client test-opt))
;      (update :checker #(checker/compose {:workload %
;                                          :timeline (timeline/html)}))
      ))
