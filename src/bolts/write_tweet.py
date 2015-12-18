
from streamparse.bolt import BatchingBolt
from cassandra.cluster import Cluster


class WriteTweetBolt(BatchingBolt):

    def initialize(self, conf, ctx):
        self.client = SimpleClient()
        self.client.connect(['54.201.160.30'])
        self.auto_ack=True

    def group_key(self, tup):
        return tup.values[0]

    def process_batch(self, key, tups):

        follower = self.client.getFollowers(key)
        for t in tups:
            tw = getattr(t, 'values')[1]

            self.client.addTimeline( tw.get('username'), tw.get('username'),
                            tw.get('fullname'),
                            tw.get('body'),
                            tw.get('created_at'),
                            tw.get('tweetid'))
            for r in follower:

                self.client.addTimeline( r[0], tw.get('username'),
                                tw.get('fullname'),
                                tw.get('body'),
                                tw.get('created_at'),
                                tw.get('tweetid'))

                self.client.addTweet( tw.get('username'),
                                tw.get('fullname'),
                                tw.get('body'),
                                tw.get('created_at'),
                                tw.get('tweetid'))

class SimpleClient(object):
    session = None

    def connect(self, nodes):
        cluster = Cluster(nodes)
        self.session = cluster.connect()

    def addTweet(self,username,fullname,body,created_at,tweetid):
        body = body.replace("'", "")
        query = "INSERT INTO twitter.Tweets(username, fullname, body, created_at, tweetid)"\
            "VALUES ('"'%s'"','"'%s'"','"'%s'"','"'%s'"', %s);" % (username, fullname, body, created_at, tweetid)

        self.session.execute(query)


    def addTimeline(self, to, username, fullname, body, created_at, tweetid):
        body = body.replace("'", "")

        query = "INSERT INTO twitter.Timeline( " \
            "for, username, fullname, tweetid, created_at, body ) "\
            "VALUES ('"'%s'"', '"'%s'"', '"'%s'"', %s, '"'%s'"', '"'%s'"');" % (to, username, fullname, tweetid, created_at, body)

        self.session.execute(query)

    def addUser(self, username, fullname, password):
        query = "INSERT INTO twitter.Users(username, fullname, pass) VALUES('"'%s'"','"'%s'"','"'%s'"');"%(username,fullname,password)

        self.session.execute(query)

    def getFollowers(self, username):
        query = "SELECT follower FROM twitter.Followers WHERE following='"'%s'"';"%(username)

        return self.session.execute(query)

    def close(self):
        self.session.cluster.shutdown()

