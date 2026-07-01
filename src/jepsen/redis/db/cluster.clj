(ns jepsen.redis.db.cluster
  "Automation for setting up Redis Cluster. Uses whatever version of Redis
  Debian has right now."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c :refer [|]]
                    [core :as jepsen]
                    [db :as db]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.redis [client :as rc]]
            [jepsen.redis.db.single :as single
             :refer [bin
                     config-file
                     data-dir
                     install!
                     log-file
                     port]]
            [clj-commons.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def cluster-create-log
  (str "/tmp/jepsen/redis-cluster-create.log"))

(def ip->node-cache
  "A mapping of IP addresses to nodes. We need this because we tell Redis
  everything in terms of IPs, but need to figure out which nodes it's talking
  about in cluster-info."
  (atom {}))

(defn ip->node
  "Takes an IP, returns the corresponding node. Only works after DB setup."
  [ip]
  (get @ip->node-cache ip))

(defn ip
  "Like control.net/ip, but caches the node as a side effect."
  [node]
  (let [ip (cn/ip node)]
    (swap! ip->node-cache assoc ip node)
    ip))

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
            (when-let [u (:username test)]
              [:--user u])
            (when-let [p (:password test)]
              [:--pass p])
            :--cluster :create
            (mapv (fn [node]
                    (str (ip node) ":" port))
                  (:nodes test))
            :--cluster-replicas (:cluster-replicas test 1)
            | :tee cluster-create-log)
    (info "Cluster created")))

(defn parse-cluster-primaries
  "Takes a cluster-info string and returns a list of primary nodes."
  [s]
  (->> (str/split-lines s)
       (keep (fn [line]
               (let [[id host+ flags] (str/split line #" ")
                     [m ip port cport] (re-find #"^(.+?):(\d+)@(\d+)" host+)
                     flags (str/split flags #",")]
                 (when (some #{"master"} flags)
                   (ip->node ip)))))
       (into [])))

(defrecord DB []
  db/DB
  (setup! [this test node]
    (install!)
    ; It's going to start right away; we need to kill it so we can configure.
    (db/kill! this test node)
    (configure! test node)
    (db/start! this test node)
    (cu/await-tcp-port (ip node) 6379 {})
    (jepsen/synchronize test)
    (when (= (jepsen/primary test) node)
      (cluster! test node))
    (rc/await-ready node))

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
    (info "Starting redis-server")
    (c/su (c/exec :service "redis-server" :start)))

  (kill! [this test node]
    (info "Killing redis-server")
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
    (->> (:nodes test)
         (pmap (fn [node]
                 (let [conn (rc/open node)]
                   (try
                     (parse-cluster-primaries (wcar conn (car/cluster-nodes)))
                     (catch Exception e
                       ; We're going to constantly have issues with down nodes;
                       ; that's fine.
                       nil)
                     (finally (rc/close! conn))))))
         (mapcat identity)
         set)))

(defn db
  "Constructs a new single-node DB."
  [opts]
  (DB.))
