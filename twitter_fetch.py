from __future__ import print_function
import tweepy
import json
from pymongo import MongoClient

MONGO_HOST= 'mongodb://localhost/indvpakdb'  
 
WORDS = ['#CT17','#INDvPAK','#PAKvIND','#ChampionsTrophyFinal','#CT2017Final']
 
CONSUMER_KEY = "lMmGKxggn9pIRvzqAKZWJVlOf"
CONSUMER_SECRET = "1dakasiuyFDxhYpm3AmLmOPoAeUSa2Zf6YMVGpKpppRiXsaJqA"
ACCESS_TOKEN = "871793972573818880-CIQDIa7cpyNf5RncsqBZ7jKuQXT9LEp"
ACCESS_TOKEN_SECRET = "pK86uk4tnO1N2a6b4F2PPeggRIFwsvc1RZcui1Hwq5IbY"


class StreamListener(tweepy.StreamListener):    
    def on_connect(self):
        print("You are now connected to the streaming API.")
    def on_error(self, status_code):
        print('An Error has occured: ' + repr(status_code))
        return False
    def on_data(self, data):
        try:
            client = MongoClient(MONGO_HOST)
            db = client.indvpakdb
            datajson = json.loads(data)
            created_at = datajson['created_at']
            print("Tweet collected at " + str(created_at))
            
            db.finals_search.insert(datajson)
            #cursor = twitter_search.find().limit(200).sort([("created_at",-1)])
        except Exception as e:
           print(e)
 
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True)) 
streamer = tweepy.Stream(auth=auth, listener=listener)
print("Tracking: " + str(WORDS))
streamer.filter(track=WORDS)
