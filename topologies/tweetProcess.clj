(ns tweetProcess
  (:use [backtype.storm.clojure]
        [tweet.spouts.tweet_spout :only [spout] :rename {spout tweet-spout}]
        [streamparse.specs])
  (:gen-class))


(defn tweetProcess [options]
   [
    ;; spout configurations
    {"tweet-spout" (spout-spec tweet-spout :p 1)}

    ;; bolt configurations
    {
      ;; Technically, this bolt isn't really needed, just need to add a proper
      ;; deserializer to Kafka spout so that it doesn't stupidly treat all
      ;; messages as strings, but this is fine for a demo
     "tweet-deserializer-bolt" (python-bolt-spec
        options
        {"tweet-spout" :shuffle}
        "bolts.tweet_deserializer.TweetDeserializerBolt"
        ["username" "json" ]
        :p 1)

      "write-tweet-bolt" (python-bolt-spec
        options
        ;; fields grouping on url
        {"tweet-deserializer-bolt" ["username"]}
        "bolts.write_tweet.WriteTweetBolt"
        ;; terminal bolt
        []
        :p 1
        :conf {"topology.tick.tuple.freq.secs", 1})

      "hash-parse-bolt" (python-bolt-spec
        options
        ;; fields grouping on url
        {"tweet-deserializer-bolt" :shuffle }
        "bolts.parse_hashtag.ParseHashtagBolt"
        ;; terminal bolt
        ["hashtag"]
        :p 1
        :conf {"topology.tick.tuple.freq.secs", 1})

      "write-hash-bolt" (python-bolt-spec
        options
        ;; fields grouping on url
        {"hash-parse-bolt" ["hashtag"]  }
        "bolts.write_hashtag.WriteHashtagBolt"
        ;; terminal bolt
        ["tag" "val"]
        :p 1
        :conf {"topology.tick.tuple.freq.secs", 1})


      "write-best-bolt" (python-bolt-spec
        options
        ;; fields grouping on url
        {"write-hash-bolt" :global }
        "bolts.write_best.WriteBestBolt"
        ;; terminal bolt
        []
        :p 1
        :conf {"topology.tick.tuple.freq.secs", 15})
    }
  ]
)
