import sys
import random
from collections import defaultdict
import tweepy
import csv

API_KEY = "UpIrXJmpUFS1hjdonEH0Gwxz6"
API_KEY_SECRET = "r9EG3X3iBoajcNCyVKNtPZIEJm2Y4NqRNHEel54xSQDNbZ4jhy"
ACCESS_TOKEN = "1277688586943356929-lPAkpNDqL5esb46dE7e2AQKe6Vju1v"
ACCESS_TOKEN_SECRET = "dCT3tHSPYYzOQMgCnSFxk6hUdwCB28yx0fuiyOc4E7QVr"

TOPIC_LIST = ['Pandemic', 'SocialDistancing', 'StayAtHome', 'Quarantine', 'Party', 'Boring']


class RSA(tweepy.StreamListener):
    def __init__(self, export_file):
        tweepy.StreamListener.__init__(self)
        self.tweet_data = defaultdict(list)
        self.idx = 0
        self.tags = defaultdict(lambda: 0)
        self.export_file = export_file
        self.rmidx = set()
        self.__init_result()

    def __init_result(self):
        with open(self.export_file, "w") as p:
            csv.writer(p)

    def construct_data(self, dl):
        tmp = []
        if len(dl) > 0:
            for i in dl:
                tag = i.get("text")
                self.tags[tag] += 1
                tmp.append(tag)
        else:
            self.rmidx.add(self.idx)

        self.tweet_data[self.idx] = tmp

    def update_data(self, dl):
        tmp = []
        if len(dl) > 0:
            if len(self.rmidx) > 0:
                pos = random.choice(list(self.rmidx))
                self.rmidx.discard(pos)
            else:
                pos = random.randint(0, 99)

            for i in dl:
                tag = i.get("text")
                self.tags[tag] += 1
                tmp.append(tag)

            if len(self.tweet_data[pos]) > 0:
                nrt = self.tweet_data[pos]
                for tag in nrt:
                    self.tags[tag] -= 1
                    if self.tags[tag] == 0:
                        self.tags.pop(tag)

            self.tweet_data[pos] = tmp
            return True
        return False

    def export(self):
        sorted_tags = sorted(self.tags.items(), key=lambda x: (-x[1], x[0]))
        top3 = sorted(set(self.tags.values()), reverse=True)[:3]
        with open(self.export_file, "w") as f:
            f.write("The number of tweets with tags from the beginning: {}\n".format(self.idx))
            for k1, v1 in sorted_tags:
                if v1 in top3:
                    f.write(k1 + " : " + str(v1) + "\n")
            f.write("\n")
            f.close()

    def on_status(self, status):
        tag_dl = status.entities.get("hashtags")

        if self.idx <= 100:
            self.construct_data(tag_dl)
            self.idx +=1
        else:
            prob = float(100.0 / self.idx)
            if random.random() < prob:
                update = self.update_data(tag_dl)
                if update:
                    self.export()
                    self.idx += 1


if __name__=="__main__":
    output_file = sys.argv[2]

    listeners = RSA(output_file)

    authentication_handler = tweepy.OAuthHandler(API_KEY, API_KEY_SECRET)
    authentication_handler.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    stream = tweepy.Stream(auth= authentication_handler, listener= listeners)

    stream.filter(track=TOPIC_LIST, languages=["en"])
