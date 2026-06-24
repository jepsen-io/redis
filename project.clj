(defproject jepsen.redis "0.2.0-SNAPSHOT"
  :description "Jepsen tests for Redis"
  :url "https://github.com/jepsen-io/redis"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [jepsen "0.3.12-SNAPSHOT"]
                 [com.taoensso/carmine "3.5.0"]]
  :jvm-opts ["-Djava.awt.headless=true"]
  :repl-options {:init-ns jepsen.redis.core}
  :main jepsen.redis.core)
