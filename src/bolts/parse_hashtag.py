
from streamparse.bolt import Bolt
import re

class ParseHashtagBolt(Bolt):

    truncating = ':00.000Z'

    def initialize(self, conf, ctx):
        pass

    def process(self, tup):
        tw = getattr(tup, 'values')[1]

        for tag in re.findall(('(#\w+)'), tw['body']) :
            self.emit([tag])
