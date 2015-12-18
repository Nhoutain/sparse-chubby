(ns tweet.spouts.tweet_spout
  (:import [storm.kafka SpoutConfig KafkaSpout KafkaConfig KafkaConfig
                        StringScheme ZkHosts]
           [backtype.storm.spout SchemeAsMultiScheme]
           [kafka.api OffsetRequest]))


;; Config spelled out below here to get the reader more comfortable with
;; Clojure
;; ^{...} adds metadata to a var definition
(def ^{:doc "Host string for Zookeeper"}
  zk-hosts "172.31.43.133:2181"
  )


;; Default path, shouldn't be a need to change this
(def ^{:doc "Zookeeper broker path"}
  zk-broker-path "/brokers")

(def kafka-zk-hosts (ZkHosts. zk-hosts zk-broker-path))

(def ^{:doc "Topic name"}
  topic-name "analytics")

(def ^{:doc "Root path of Zookeeper for spout to store consumer offsets"}
  kafka-zk-root "/kafka_storm")

(def ^{:doc "ID for this Kafka consumer"}
  kafka-consumer-id "stoup")

(def ^{:doc "Kafka spout config definition"}
  spout-config (let [cfg (SpoutConfig. kafka-zk-hosts topic-name kafka-zk-root kafka-consumer-id)]
                  (set! (. cfg scheme) (SchemeAsMultiScheme. (StringScheme.)))
                  ;; During testing, it's usually valuable to force a spout to
                  ;; read from the beginning of a topic
                  (set! (. cfg forceFromStart) false)
                  (set! (. cfg startOffsetTime) -2 );;(. OffsetRequest LatestTime))
                  cfg))

(def spout (KafkaSpout. spout-config))
