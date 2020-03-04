(ns jepsen.redis.db
  "Database automation"
  (:require [clojure.java [io :as io]
                          [shell :as shell]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [parse-long]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]]))

(def build-dir
  "A remote directory for us to clone projects and compile them."
  "/tmp/jepsen/build")

(def redis-raft-repo
  "Where can we clone redis-raft from?"
  "git@github.com:RedisLabs/redisraft.git")

(def redis-repo
  "Where can we clone redis from?"
  "git@github.com:antirez/redis.git")

(def dir
  "The remote directory where we deploy redis to"
  "/opt/redis")

(def build-file
  "A file we create to track the last built version; speeds up compilation."
  "jepsen-built-version")

(def log-file       (str dir "/redis.log"))
(def pid-file       (str dir "/redis.pid"))
(def binary         "redis-server")
(def cli-binary     "redis-cli")
(def db-file        "redis.rdb")
(def raft-log-file  "raftlog.db")
(def config-file    "redis.conf")

(defn install-build-tools!
  "Installs prerequisite packages for building redis and redisraft."
  []
  (debian/install [:build-essential :cmake :libbsd-dev :libtool :autoconf :automake]))

(defn checkout-repo!
  "Checks out a repo at the given version into a directory in build/ named
  `dir`. Returns the path to the build directory."
  [repo-url dir version]
  (let [full-dir (str build-dir "/" dir)]
    (when-not (cu/exists? full-dir)
      (c/cd build-dir
            (info "Cloning into" full-dir)
            (c/exec :mkdir :-p build-dir)
            (c/exec :git :clone repo-url dir)))

    (c/cd full-dir
          (try+ (c/exec :git :checkout version)
                (catch [:exit 1] e
                  (if (re-find #"pathspec .+ did not match any file" (:err e))
                    (do ; Ah, we're out of date
                        (c/exec :git :fetch)
                        (c/exec :git :checkout version))
                    (throw+ e)))))
    full-dir))

(def build-locks
  "We use these locks to prevent concurrent builds."
  (util/named-locks))

(defmacro with-build-version
  "Takes a test, a repo name, a version, and a body. Builds the repo by
  evaluating body, only if it hasn't already been built. Takes out a lock on a
  per-repo basis to prevent concurrent builds. Remembers what version was last
  built by storing a file in the repo directory. Returns the result of body if
  evaluated, or the build directory."
  [node repo-name version & body]
  `(util/with-named-lock build-locks [~node ~repo-name]
     (let [build-file# (str build-dir "/" ~repo-name "/" build-file)]
       (if (try+ (= (str ~version) (c/exec :cat build-file#))
                (catch [:exit 1] e# ; Not found
                  false))
         ; Already built
         (str build-dir "/" ~repo-name)
         ; Build
         (let [res# (do ~@body)]
           ; Log version
           (c/exec :echo ~version :> build-file#)
           res#)))))

(defn build-redis-raft!
  "Compiles redis-raft from source, and returns the directory we built in."
  [test node]
  (let [version (:raft-version test)]
    (with-build-version node "redis-raft" version
      (let [dir (checkout-repo! redis-raft-repo "redis-raft" version)]
        (info "Building redis-raft" (:raft-version test))
        (c/cd dir
          (c/exec :git :submodule :init)
          (c/exec :git :submodule :update)
          (c/exec :make :clean)
          (c/exec :make))
        dir))))

(defn build-redis!
  "Compiles redis, and returns the directory we built in."
  [test node]
  (let [version (:version test)]
    (with-build-version node "redis" version
      (let [dir (checkout-repo! redis-repo "redis" (:version test))]
        (info "Building redis" (:version test))
        (c/cd dir
          (c/exec :make :distclean)
          (c/exec :make))
        dir))))

(defn deploy-redis!
  "Uploads redis binaries built from the given directory."
  [build-dir]
  (info "Deploying redis")
  (c/exec :mkdir :-p dir)
  (doseq [f ["redis-server" "redis-cli"]]
    (c/exec :cp (str build-dir "/src/" f) (str dir "/"))
    (c/exec :chmod "+x" (str dir "/" f))))

(defn deploy-redis-raft!
  "Uploads redis binaries built from the given directory."
  [build-dir]
  (info "Deploying redis-raft")
  (c/exec :mkdir :-p dir)
  (doseq [f ["redisraft.so"]]
    (c/exec :cp (str build-dir "/" f) (str dir "/"))))

(defn cli!
  "Runs a Redis CLI command. Includes a 1s timeout."
  [& args]
  (c/su (apply c/exec :timeout "1s" (str dir "/" cli-binary) args)))

(defn raft-info-str
  "Returns the current cluster state as a string."
  []
  (cli! :--raw "RAFT.INFO"))

(defn parse-raft-info-node
  "Parses a node string in a raft-info string, which is a list of k=v pairs."
  [s]
  (->> (str/split s #",")
       (map (fn [part]
              (let [[k v] (str/split part #"=")
                    k (keyword k)]
                [k (case k
                     (:id :last_conn_secs :port :conn_errors :conn_oks)
                     (parse-long v)
                     v)])))
       (into {})))

(defn parse-raft-info-kv
  "Parses a string key and value in a section of the raft info string into a
  key path (for assoc-in) and value [ks v]."
  [section k v]
  (let [k (keyword k)]
    (case section
      :raft     (case k
                  (:role :state) [[section k] (keyword v)]

                  (:node_id :leader_id :current_term :num_nodes)
                  [[section k] (parse-long v)]

                  (if (re-find #"^node(\d+)$" (name k))
                    ; This is a node1, node2, ... entry; file it in :nodes
                    [[section :nodes k] (parse-raft-info-node v)]
                    [[section k] v]))
      :log      [[section k] (parse-long v)]
      :snapshot [[section k] v]
      :clients  (case k
                  :clients_in_multi_state [[section k] (parse-long v)]
                  [[section k] v])
      [[section k] v])))

(defn raft-info
  "Current cluster state as a map."
  []
  (-> (raft-info-str)
      str/split-lines
      (->> (reduce (fn [[s section] line]
                     (if (re-find #"^\s*$" line)
                       ; Blank
                       [s section]

                       (if-let [m (re-find #"^# (.+)$" line)]
                         ; New section
                         (let [section (keyword (.toLowerCase (m 1)))]
                           [s section])

                         (if-let [[_ k v] (re-find #"^(.+?):(.+)$" line)]
                           ; k:v pair
                           (let [[ks v] (parse-raft-info-kv section k v)]
                             [(assoc-in s ks v) section])

                           ; Don't know
                           (throw+ {:type :raft-info-parse-error
                                    :line line})))))
                   [{} nil]))
       first
       ; Drop node keys; they're not real
       (update-in [:raft :nodes] vals)))

(def node-ips
  "Returns a map of node names to IP addresses. Memoized."
  (memoize
    (fn [test]
      (c/on-nodes test (fn [test node]
                         (-> (c/exec :getent :hosts node)
                             (str/split #"\s+")
                             first))))))

(defn node-state
  "This is a bit tricky. Redis-raft lets every node report its own node id, as
  well as the ids and *IP addresses* of other nodes--but the node set doesn't
  include the node you're currently talking to, so we have to combine info from
  multiple parts of the raft-info response. In addition, we use node names,
  rather than IPs, so we have to map those back and forth too.

  At the end of all this, we return a collection of maps, each like

  {:node \"n1\"
   :id   123}

  This is a best-effort function; when nodes are down we can't get information
  from them. Results may be partial."
  [test]
  (let [ip->node (into {} (map (juxt val key) (node-ips test)))
        states (c/on-nodes test
                           (fn [test node]
                             ; We take our local raft info, and massage it
                             ; into a set of {:node n1, :id 123} maps,
                             ; combining info about the local node and
                             ; other nodes.
                             (let [ri (raft-info)
                                   r  (:raft ri)]
                               ; Other nodes
                               (->> (:nodes r)
                                    (map (fn [n]
                                           {:id   (:id n)
                                            :node (ip->node (:addr n))}))
                                    ; Local node
                                    (cons {:id  (:node_id r)
                                           :node node})))))]
    ; Now we make these distinct by node.
    (->> states
         vals
         (apply concat)
         (map (juxt :node identity))
         (into {})
         vals)))

(defn node-id
  "Looks up the numeric ID of a node by name."
  [node]
  (throw+ {:type :todo}))

(defn remove-node!
  "Removes a node from the cluster by node name."
  [node]
  (cli! "RAFT.NODE" "REMOVE" (node-id node)))

(defprotocol Membership
  "Allows a database to support node introspection, growing, and shrinking, the
  cluster."
  (members  [db]      "The set of nodes currently in the cluster.")
  (join!    [db node] "Add a node to the cluster.")
  (leave!   [db node] "Removes a node from the cluster."))

(defn redis-raft
  "Sets up a Redis-Raft based cluster. Tests should include a :version option,
  which will be the git SHA or tag to build."
  []
  (reify db/DB
    (setup! [this test node]
      (c/su
        ; This is a total hack, but since we're grabbing private repos via SSH,
        ; we gotta prime SSH to know about github. Once this repo is public
        ; this nonsense can go away. Definitely not secure, but are we REALLY
        ; being MITMed right now?
        ; (c/exec :ssh-keyscan :-t :rsa "github.com"
        ;        :>> (c/lit "~/.ssh/known_hosts"))

        ; Build and install
        (install-build-tools!)
        (let [redis      (future (-> test (build-redis! node) deploy-redis!))
              redis-raft (future (-> test (build-redis-raft! node)
                                    deploy-redis-raft!))]
          [@redis @redis-raft]

          ; Start
          (db/start! this test node)
          (Thread/sleep 1000) ; TODO: block until port bound

          (if (= node (jepsen/primary test))
            ; Initialize the cluster on the primary
            (do (cli! :raft.cluster :init)
                (info "Main init done, syncing")
                (jepsen/synchronize test 600)) ; Compilation can be slow
            ; And join on secondaries.
            (do (info "Waiting for main init")
                (jepsen/synchronize test 600) ; Ditto
                (info "Joining")
                ; Port is mandatory here
                (cli! :raft.cluster :join (str (jepsen/primary test) ":6379"))))

          (Thread/sleep 2000)
          (info :raft-info (raft-info))
          (info :node-state (with-out-str (pprint (node-state test))))
          )))

    (teardown! [this test node]
      (db/kill! this test node)
      (c/su (c/exec :rm :-rf dir)))

    db/Primary
    (setup-primary! [_ test node])

    (primaries      [_ test]
      (->> (c/on-nodes test
                       (fn [test node]
                         (re-find #"role:leader" (raft-info-str))))
           (filter val)
           (map key)))

    db/Process
    (start! [_ test node]
            (c/su
              (info :starting :redis)
              (cu/start-daemon!
                {:logfile log-file
                 :pidfile pid-file
                 :chdir   dir}
                binary
                ; config-file
                :--bind               "0.0.0.0"
                :--dbfilename         db-file
                :--loadmodule         (str dir "/redisraft.so")
                (str "raft-log-filename=" raft-log-file)
                (when (:follower-proxy test) (str "follower-proxy=yes"))
                )))

    (kill! [_ test node]
      (c/su
        (cu/stop-daemon! binary pid-file)))

    db/Pause
    (pause!  [_ test node] (c/su (cu/grepkill! :stop binary)))
    (resume! [_ test node] (c/su (cu/grepkill! :cont binary)))

    db/LogFiles
    (log-files [_ test node]
      [log-file
       (str dir "/" db-file)
       (str dir "/" raft-log-file)])))
