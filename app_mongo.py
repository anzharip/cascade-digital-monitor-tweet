# YouTube Video: https://www.youtube.com/watch?v=wlnx-7cm4Gg
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
 
import env_file

# For Firestore unique document name
import uuid

# To convert str data to object
import json

# Import PyMongo
from pymongo import MongoClient
import urllib.parse

username = urllib.parse.quote_plus(env_file.MONGODB_USERNAME)
password = urllib.parse.quote_plus(env_file.MONGODB_PASSWORD)

client = MongoClient('mongodb://%s:%s@103.92.104.173:32773' % (username, password))
db = client[env_file.MONGODB_DATABASE]
 
# # # # TWITTER STREAMER # # # #
class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """
    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = StdOutListener(fetched_tweets_filename)
        auth = OAuthHandler(env_file.CONSUMER_KEY, env_file.CONSUMER_SECRET)
        auth.set_access_token(env_file.ACCESS_TOKEN, env_file.ACCESS_TOKEN_SECRET)
        stream = Stream(auth, listener)

        # This line filter Twitter Streams to capture data by the keywords: 
        stream.filter(track=hash_tag_list)


# # # # TWITTER STREAM LISTENER # # # #
class StdOutListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            mongodb_posts = db['tweets']
            post = json.loads(data)
            post_id = mongodb_posts.insert_one(post).inserted_id
            # data is str type
            print(data)
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True
          

    def on_error(self, status):
        print(status)

 
if __name__ == '__main__':
 
    # Authenticate using config.py and connect to Twitter Streaming API.
    hash_tag_list = env_file.HASHTAG_LIST
    fetched_tweets_filename = env_file.FETCHED_TWEETS_FILENAME

    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list)