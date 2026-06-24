(ns jepsen.redis.db.single
  "Automation for setting up a single Redis node. Uses whatever Debian has
  right now."
  (:require [clojure.java.io :as io]
            [jepsen [control :as c]
                    [db :as db]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [clj-commons.slingshot :refer [try+ throw+]]))

(def port
  "The port Redis binds."
  6379)

(def bin
  "The full path to redis-server"
  "/usr/bin/redis-server")

(def log-file
  "The file Redis logs to."
  "/var/log/redis/redis-server.log")

(def data-dir
  "Where Redis writes files."
  "/var/lib/redis")

(defn install!
  "Installs redis via debian on the local node."
  []
  (c/su
    (debian/install ["redis-server" "redis-tools"])))

(defn configure!
  "Uploads config files."
  [test node]
  (c/su
      (-> (io/resource "single.conf")
          slurp
          (cu/write-file!
            "/etc/redis/redis.conf"))))

(defrecord DB []
  db/DB
  (setup! [this test node]
    (assert (= 1 (count (:nodes test))))
    (install!)
    ; It's going to start right away; we need to kill it so we can configure.
    (db/kill! this test node)
    (configure! test node)
    (db/start! this test node)
    (cu/await-tcp-port 6379))

  (teardown! [this test node]
    (db/kill! this test node)
    (c/su (c/exec :rm :-rf (c/lit (str data-dir "/*")))))

  db/LogFiles
  (log-files [this test node]
             {log-file "redis.log"})

  db/Kill
  (start! [this test node]
    (c/su (c/exec :service "redis-server" :start)))

  (kill! [this test node]
    (cu/kill-bin! :KILL true bin)
    (try+ (c/su (c/exec :service "redis-server" :stop))
          (catch [:exit 1] e
            :service-does-not-exist)))

  db/Pause
  (pause! [this test node]
    (cu/kill-bin! :STOP false bin))

  (resume! [this test node]
    (cu/kill-bin! :CONT false bin)))

(defn db
  "Constructs a new single-node DB."
  []
  (DB.))
