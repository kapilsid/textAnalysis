from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import json

import configparser

from kafka import KafkaProducer


config = configparser.ConfigParser()
config.read('twitter.ini')

consumer_key = config['twitter']['consumer_key']
consumer_secret = config['twitter']['consumer_secret']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']

p = KafkaProducer(bootstrap_servers=['localhost:9093'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        
#setup_twitter_oauth(api_key,api_secret,access_token,access_token_secret)

#positive=scan('positive-words.txt',what='character',comment.char=';')
#negative=scan('negative-words.txt',what='character',comment.char=';')

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)

class KafkaListener(StreamListener):

    def on_data(self, data):
        p.send('tweets', data)        
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    k = KafkaListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, k)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['python', 'javascript', 'ruby'])
