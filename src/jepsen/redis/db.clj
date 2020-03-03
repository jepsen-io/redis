(ns jepsen.redis.db
  "Database automation"
  (:require [clojure.java [io :as io]
                          [shell :as shell]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util]]
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
          (c/exec :git :checkout version))
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
  "Runs a Redis CLI command"
  [& args]
  (c/su (apply c/exec (str dir "/" cli-binary) args)))

(defn raft-info
  "Returns the current cluster state."
  []
  (cli! :--raw "RAFT.INFO"))

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

          ;(info :raft-info (raft-info))
          )))

    (teardown! [this test node]
      (db/kill! this test node)
      (c/su (c/exec :rm :-rf dir)))

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
