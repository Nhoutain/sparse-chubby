
from streamparse.bolt import BatchingBolt
import redis
import operator


class WriteBestBolt(BatchingBolt):

    def initialize(self, conf, ctx):
        self.r = redis.StrictRedis(host='54.201.160.30', port=6379)
        self.sort = {}

    def group_key(self, tup):
        return tup.values[0]

    def process_batch(self, key, tups):
        val = sum([ getattr(v, 'values')[1] for v in tups ])
        self.sort[key] = val + self.sort.get(key, 0)

    def process_batches(self):
        self.sort = {}
        super(WriteBestBolt, self).process_batches()

        self.log(self.sort)

        for (i, (k, v)) in enumerate(sorted(self.sort.items(), key=operator.itemgetter(1),
                                    reverse=True)[:10]):
            self.r.set(i, k)
            self.r.expire(i, 80)

