(ns jepsen.redis.nemesis
  "Nemeses for Redis."
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [db :as db]
                    [generator :as gen]
                    [nemesis :as n]
                    [net :as net]
                    [util :as util]]
            [jepsen.nemesis [combined :as nc]]))

(defn package-for
  "Builds a combined package for the given options."
  [opts]
  (->> (nc/nemesis-packages opts)
       (remove nil?)
       nc/compose-packages))

(defn package
  "Takes CLI opts; constructs a nemesis and generators for Redis. Options:

    :interval   How long to wait between operations.
    :db         The database to add/remove nodes from.
    :faults     The set of faults."
  [opts]
  (let [package (package-for opts)]
    package))
