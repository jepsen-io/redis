(ns jepsen.redis.nemesis
  "Nemeses for Redis"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [generator :as gen]
                    [nemesis :as n]
                    [net :as net]
                    [util :as util]]
            [jepsen.nemesis [time :as nt]
                            [combined :as nc]]
            [jepsen.redis [db :as db]]))

(defn member-nemesis
  "A nemesis for adding and removing nodes from the cluster."
  [])

(defn package
  "Takes CLI opts; constructs a nemesis and generators for Redis."
  [opts]
  (info opts)
  (-> (nc/nemesis-packages opts)
      nc/compose-packages))
