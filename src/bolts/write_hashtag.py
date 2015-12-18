
from streamparse.bolt import TicklessBatchingBolt


class WriteHashtagBolt(TicklessBatchingBolt):

    secs_between_batches = 30

    def initialize(self, conf, ctx):
        self.auto_ack=True
        self.counters = [{} for i in range(11)]
        self.index = 0


    def group_key(self, tup):
        return self.index

    def process(self, tup):
        self.counters[self.index][tup.values[0]] = 1 + \
            self.counters[self.index].get(tup.values[0], 0)
        self._batches[self.index].append(tup)

    def process_batches(self):
        super(WriteHashtagBolt, self).process_batches()
        self.incr_index()

    def process_batch(self, index, tups):

        for key in self.counters[index].keys():
            if((self.index + 1) % 11 == index):
                self.emit([key, - self.counters[index][key]])
            else:
                self.emit([key, self.counters[index][key]])

    def incr_index(self):
        self.index = (self.index + 1) % 11
        self.counters[self.index] = {}

        for i in range(11):
            self._batches[i] = []

