(ns jepsen.redis.db.cluster
  "Automation for setting up Redis Cluster. Uses whatever version of Redis
  Debian has right now."
  (:require [clojure [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c :refer [|]]
                    [core :as jepsen]
                    [db :as db]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.redis.db.single :as single
             :refer [bin
                     config-file
                     data-dir
                     install!
                     log-file
                     port]]
            [clj-commons.slingshot :refer [try+ throw+]]))

(def cluster-create-log
  (str "/tmp/jepsen/redis-cluster-create.log"))

(defn config-str
  "Returns a string for this node's config file."
  [test node]
  (str (single/config-str test node)
       "\n"
       (-> (io/resource "cluster.conf")
           slurp
           (str/replace #"%APPEND_ONLY%"
                        (if (:append-only test true)
                          "yes"
                          "no")))))
(defn configure!
  "Uploads config files."
  [test node]
  (c/su (cu/write-file! (config-str test node) "/etc/redis/redis.conf")))

(defn cluster!
  "Creates a cluster."
  [test node]
  (c/su
    (info "Creating redis cluster")
    (c/exec :yes "yes" |
            :redis-cli :-h node
            :--cluster :create
            (mapv (fn [node]
                    (str (cn/ip node) ":" port))
                  (:nodes test))
            :--cluster-replicas (:cluster-replicas test 1)
            | :tee cluster-create-log)
    (info "Cluster created")))

(defrecord DB []
  db/DB
  (setup! [this test node]
    (install!)
    ; It's going to start right away; we need to kill it so we can configure.
    (db/kill! this test node)
    (configure! test node)
    (db/start! this test node)
    (jepsen/synchronize test)
    (when (= (jepsen/primary test) node)
      (cluster! test node))
    (cu/await-tcp-port (cn/ip node) 6379 {}))

  (teardown! [this test node]
    (db/kill! this test node)
    (c/su (c/exec :rm :-rf
                  (c/lit (str data-dir "/*"))
                  log-file
                  cluster-create-log)))

  db/LogFiles
  (log-files [this test node]
             {config-file        "redis.conf"
              log-file           "redis.log"
              cluster-create-log "cluster-create.log"})

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
    (cu/kill-bin! :CONT false bin)))

(defn db
  "Constructs a new single-node DB."
  [opts]
  (DB.))
