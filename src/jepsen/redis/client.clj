(ns jepsen.redis.client
  "Helper functions for working with Carmine, our redis client."
  (:require [clojure.tools.logging :refer [info warn]]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]
            [taoensso.carmine [connections :as conn]]))

; There's a lot of weirdness in the way Carmine handles connection pooling;
; you're expected to pass around maps which *specify* how to build a connection
; pool, and it maps that to a connection pool under the hood--I think via
; connections/conn-pool memoization. What worries me is that that memoization
; might return connection pools for *other* clients, royally screwing up our
; open/close logic.

; Now, I can't figure out a way to just get you know, a Plain old Connection
; out of Carmine safely. So what we're gonna do here is cheat: we build a
; Carmine pool that builds a fresh conn on every call to get-conn, pull a
; connection out of it, throw away the pool (which is not in general a good
; idea, but we know what we're doing), and wrap that connection in our *own*
; pool. This doesn't have to do any actual connection tracking because we only
; ever have one thread interact with a pool at a time.

(defrecord SingleConnectionPool [conn]
  conn/IConnectionPool
  (get-conn [_ spec] conn)

  (release-conn [_ conn])

  (release-conn [_ conn exception])

  java.io.Closeable
  (close [_] (conn/close-conn conn)))

(defn open
  "Opens a connection to a node. Our connections are Carmine IConnectionPools."
  [node]
  (let [spec {:host       node
              :port       6379
              :timeout-ms 10000}
        seed-pool (conn/conn-pool :none)
        conn      (conn/get-conn seed-pool spec)]
    {:pool (SingleConnectionPool. conn)
     :spec spec}))

(defn close!
  "Closes a connection to a node."
  [^java.io.Closeable conn]
  (.close (:pool conn)))

(defmacro with-exceptions
  "Takes an operation, an idempotent :f set, and a body; evaluates body,
  converting known exceptions to failed ops."
  [op idempotent & body]
  `(let [crash# (if (~idempotent (:f ~op)) :fail :info)]
     (try+ ~@body
           (catch [:prefix :err] e#
             (condp re-find (.getMessage (:throwable ~'&throw-context))
               ; These two would ordinarily be our fault, but are actually
               ; caused by follower proxies mangling connection state.
               #"ERR DISCARD without MULTI"
               (assoc ~op :type crash#, :error :discard-without-multi)

               #"ERR MULTI calls can not be nested"
               (assoc ~op :type crash#, :error :nested-multi)

               (throw+)))
           (catch [:prefix :moved] e#
             (assoc ~op :type :fail, :error :moved))

           (catch [:prefix :nocluster] e#
             (assoc ~op :type :fail, :error :nocluster))

           (catch [:prefix :noleader] e#
             (assoc ~op :type :fail, :error :noleader))

           (catch [:prefix :notleader] e#
             (assoc ~op :type :fail, :error :notleader))

           (catch [:prefix :timeout] e#
             (assoc ~op :type crash# :error :timeout))

           (catch java.io.EOFException e#
             (assoc ~op :type crash#, :error :eof))

           (catch java.net.ConnectException e#
             (assoc ~op :type :fail, :error :connection-refused))

           (catch java.net.SocketTimeoutException e#
             (assoc ~op :type crash#, :error :socket-timeout)))))

(defmacro with-txn
  "Runs in a multi ... exec scope. Discards body, returns the results of exec."
  [conn & body]
  `(do (wcar ~conn (car/multi))
       (try ~@body
            (wcar ~conn (car/exec))
            (catch Throwable t#
              (wcar ~conn (car/discard))
              (throw t#)))))
