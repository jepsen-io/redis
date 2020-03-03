(ns jepsen.redis.core
  "Top-level test runner, integration point for various workloads and nemeses."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.redis [append :as append]
                          [db     :as rdb]
                          [nemesis :as nemesis]]))

(def workloads
  "A map of workload names to functions that can take opts and construct
  workloads."
  {:append  append/workload})

(def standard-workloads
  "The workload names we run for test-all by default."
  (keys workloads))

(def standard-nemeses
  "Combinations of nemeses for tests"
  [[]
   [:pause :kill :partition :clock :member]])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :kill :partition :clock :member]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(defn redis-test
  "Builds up a Redis test from CLI options."
  [opts]
  (let [workload ((workloads (:workload opts)) opts)
        db        (rdb/redis-raft)
        nemesis   (nemesis/package
                    {:db      db
                     :nodes   (:nodes opts)
                     :faults  (set (:nemesis opts))
                     :partition {:targets [:majority :majorities-ring]}
                     :pause     {:targets [:one :majority]}
                     :kill      {:targets [:one :majority :all]}
                     :interval  (:nemesis-interval opts)})
        _ (info (pr-str nemesis))
        ]
    (merge tests/noop-test
           opts
           workload
           {:checker    (checker/compose
                          {:perf        (checker/perf
                                          {:nemeses (:perf nemesis)})
                           :clock       (checker/clock-plot)
                           :stats       (checker/stats)
                           :exceptions  (checker/unhandled-exceptions)
                           :workload    (:checker workload)})
            :db         db
            :generator  (->> (:generator workload)
                             (gen/stagger (/ (:rate opts)))
                             (gen/nemesis (:generator nemesis))
                             (gen/time-limit (:time-limit opts)))
            :name       (str "redis " (:version opts)
                             " (raft " (:raft-version opts) ") "
                             (when (:follower-proxy opts)
                               "proxy ")
                             (name (:workload opts)) " "
                             (str/join "," (map name (:nemesis opts))))
            :nemesis    (:nemesis nemesis)
            :os         debian/os})))

(def cli-opts
  "Options for test runners."
  [[nil "--follower-proxy" "If true, proxy requests from followers to leader."
    :default false]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill :partition :clock :member})
               "Faults must be pause, kill, partition, clock, or member, or the special faults all or none."]]

   [nil "--nemesis-interval SECONDS" "How long to wait between nemesis faults."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--raft-version VERSION" "What version of redis-raft should we test?"
    :default "1b3fbf6"]

   ["-r" "--rate HZ" "Approximate number of requests per second per thread"
    :default 10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   ["-v" "--version VERSION" "What version of Redis should we test?"
    :default "f88f866"]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn all-tests
  "Takes parsed CLI options and constructs a sequence of test options, by
  combining all workloads and nemeses."
  [opts]
  (let [nemeses     (if-let [n (:nemesis opts)]  [n] standard-nemeses)
        workloads   (if-let [w (:workload opts)] [w] standard-workloads)
        counts      (range (:test-count opts))]
    (->> (for [i counts, n nemeses, w workloads]
           (assoc opts :nemesis n :workload w))
         (map redis-test))))

(defn -main
  "Handles CLI args."
  [& args]
  (cli/run! (merge (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
