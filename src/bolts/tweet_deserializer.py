import simplejson as json
from streamparse.bolt import Bolt

class TweetDeserializerBolt(Bolt):

    def process(self, tup):
        # Exceptions are automatically caught and reported
        msg = json.loads(tup.values[0])
        self.emit([msg['username'], msg])


