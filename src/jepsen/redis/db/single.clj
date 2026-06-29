(ns jepsen.redis.db.single
  "Automation for setting up a single Redis node. Uses whatever Debian has
  right now."
  (:require [clojure [string :as str]]
            [clojure.java.io :as io]
            [jepsen [control :as c]
                    [db :as db]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
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

(def config-file
  "Where do we write the config?"
  "/etc/redis/redis.conf")

(defn install!
  "Installs redis via debian on the local node."
  []
  (c/su
    (debian/install ["redis-server" "redis-tools"])))

(defn config-str
  "Returns a string for this node's configuration file."
  [test node]
  (-> (io/resource "single.conf")
      slurp
      (str/replace #"%IP%" (cn/ip node))
      (str/replace #"%APPEND_ONLY%"
                   (if (:append-only test true)
                     "yes"
                     "no"))
      (str/replace #"%APPEND_FSYNC%" (name (:append-fsync test :everysec)))
      (cond->
        (and (:username test)
             (:password test))
        (str "\n\nrequirepass " (:password test)
             "\nuser " (:username test) " on +@all ~* >" (:password test)))))

(defn configure!
  "Uploads config files."
  [test node]
  (c/su (cu/write-file! (config-str test node) config-file)))

(defrecord DB []
  db/DB
  (setup! [this test node]
    (assert (= 1 (count (:nodes test))))
    (install!)
    ; It's going to start right away; we need to kill it so we can configure.
    (db/kill! this test node)
    (configure! test node)
    (db/start! this test node)
    (cu/await-tcp-port (cn/ip node) 6379 {}))

  (teardown! [this test node]
    (db/kill! this test node)
    (c/su (c/exec :rm :-rf
                  (c/lit (str data-dir "/*"))
                  log-file)))

  db/LogFiles
  (log-files [this test node]
             {config-file "redis.conf"
              log-file    "redis.log"})

  db/Kill
  (start! [this test node]
    (c/su (c/exec :service "redis-server" :start)))

  (kill! [this test node]
    (cu/kill-bin! :KILL true bin)
    (try+ (c/su (c/exec :service "redis-server" :stop))
          (catch (#{1 5} (:exit %)) e
            :service-does-not-exist)))

  db/Pause
  (pause! [this test node]
    (cu/kill-bin! :STOP false bin))

  (resume! [this test node]
    (cu/kill-bin! :CONT false bin))

  db/Primary
  (setup-primary! [this test node])

  (primaries [this test]
    (:nodes test)))

(defn db
  "Constructs a new single-node DB."
  [opts]
  (DB.))
