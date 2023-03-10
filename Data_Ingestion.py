import json
from kafka import SimpleProducer, KafkaClient
import octoparse
import configparser

class HespressStreamListener(octoparse.StreamListener):

    def __init__(self, api):
        self.api = api
        super(octoparse.StreamListener, self).__init__()
        client = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(client, async = True, batch_send_every_n = 1000,batch_send_every_t = 10)
    def on_status(self, status):

        msg = status.text.encode('utf-8')
        #print(msg)
        try:
           self.producer.send_messages(b'twitterstream', msg)
        except Exception as e:
           print(e)
           return False
        return True
    def on_error(self, status_code):
        print("Error received in kafka producer")
        return True # Don't kill the stream
    def on_timeout(self):
        return True # Don't kill the stream
        if __name__ == '__main__':
           # Read the credententials from 'twitter.txt' file
           config = configparser.ConfigParser()
           config.read('commentaire.txt')
           consumer_key = config['DEFAULT']['consumerKey']
           consumer_secret = config['DEFAULT']['consumerSecret']
           access_key = config['DEFAULT']['accessToken']
           access_secret = config['DEFAULT']['accessTokenSecret']
           # Create Auth object
           auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
           auth.set_access_token(access_key, access_secret)
           api = tweepy.API(auth)
           # Create stream and bind the listener to it
           stream = tweepy.Stream(auth, listener = HespressStreamListener(api))
           #Custom Filter rules pull all traffic for those filters in real time.
           #stream .filter(track = ['love', 'hate'], languages = ['en'])
           stream.filter(locations=[-180,-90,180,90], languages = ['en'])