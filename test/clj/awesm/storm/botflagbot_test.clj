(ns awesm.storm.botflagbot-test
  (:use [clojure test])
  (:import [awesm.storm.spout RandomClickEventSpout])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

(deftest test-multilang-php
  (with-local-cluster [cluster :supervisors 4]
    (let [nimbus (:nimbus cluster)
          topology (thrift/mk-topology
                      {"1" (thrift/mk-spout-spec (RandomClickEventSpout.))}
                      {"2" (thrift/mk-shell-bolt-spec {"1" :shuffle} "php" "botflag.php" ["bot"] :parallelism-hint 1)}
                      )]
      (submit-local-topology nimbus
                          "test"
                          {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 20 TOPOLOGY-MESSAGE-TIMEOUT-SECS 3 TOPOLOGY-DEBUG true}
                          topology)
      (Thread/sleep 10000)
      (.killTopology nimbus "test")
      (Thread/sleep 10000)
      ))) 