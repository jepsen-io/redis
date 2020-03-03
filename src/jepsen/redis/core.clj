(ns jepsen.redis.core
  "Top-level test runner, integration point for various workloads and nemeses."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.redis [append :as append]
                          [db     :as rdb]]))

(def workloads
  "A map of workload names to functions that can take opts and construct
  workloads."
  {:append  append/workload})

(def standard-workloads
  "The workload names we run for test-all by default."
  (keys workloads))

(def nemesis-specs
  "The types of failures our nemesis can perform."
  #{})

(defn default-nemesis?
  "Is this nemesis option map, as produced by the CLI, the default?"
  [nemesis-opts]
  (= {} (dissoc nemesis-opts :interval)))

(def standard-nemeses
  "A set of prepackaged nemeses"
  [; Nothing
   {:interval         1}])

(defn redis-test
  "Builds up a Redis test from CLI options."
  [opts]
  (let [workload ((workloads (:workload opts)) opts)]
    (merge tests/noop-test
           opts
           workload
           {:checker    (checker/compose
                          {:perf        (checker/perf)
                           :clock       (checker/clock-plot)
                           :stats       (checker/stats)
                           :exceptions  (checker/unhandled-exceptions)
                           :workload    (:checker workload)})
            :db         (rdb/redis-raft)
            :generator  (->> (:generator workload)
                             (gen/stagger (/ (:rate opts)))
                             (gen/nemesis nil)
                             (gen/time-limit (:time-limit opts)))
            :nemesis    nemesis/noop
            :os         debian/os})))

(defn parse-nemesis-spec
  "Parses a comma-separated string of nemesis types, and turns it into an
  option map like {:kill-alpha? true ...}"
  [s]
  (if (= s "none")
    {}
    (->> (str/split s #",")
         (map (fn [o] [(keyword (str o "?")) true]))
         (into {}))))

(def cli-opts
  "Options for test runners."
  [[nil "--follower-proxy" "If true, proxy requests from followers to leader."
    :default false]

    [nil  "--nemesis SPEC" "A comma-separated list of nemesis types"
    :default {:interval 10}
    :parse-fn parse-nemesis-spec
    :assoc-fn (fn [m k v]
                (update m :nemesis merge v))
    :validate [(fn [parsed]
                 (and (map? parsed)
                      (every? nemesis-specs (keys parsed))))
               (str "Should be a comma-separated list of failure types. A failure type "
                    (.toLowerCase (cli/one-of nemesis-specs))
                    ". Or, you can use 'none' to indicate no failures.")]]

   [nil "--raft-version VERSION" "What version of redis-raft should we test?"
    :default "master"]

   ["-r" "--rate HZ" "Approximate number of requests per second per thread"
    :default 10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]

   ["-v" "--version VERSION" "What version of Redis should we test?"
    :default "unstable"]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn all-tests
  "Takes parsed CLI options and constructs a sequence of test options, by
  combining all workloads and nemeses."
  [opts]
  (let [nemeses     (if-let [n (:nemesis opts)]
                      (if (default-nemesis? n) standard-nemeses [n])
                      standard-nemeses)
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
