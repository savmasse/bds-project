import tweepy
import re

class HashtagFinder():
    
    def __init__(self, api, starting_hashtag, location_radius, language, tagignore):
        self.api = api
        self.starting_hashtag = starting_hashtag
        self.location_radius = location_radius
        self.language = language
        self.hashtags = {}
        self.tagignore = tagignore
        
    def collect_tags(self, tag=""):
        if tag == "":
            tag = self.starting_hashtag
        items = tweepy.Cursor(self.api.search,
                             q = tag + " -filter:retweets",
                             geocode=self.location_radius,
                             count=100,
                             lang=self.language,
                             include_rts=False,
                             tweet_mode="extended").items(1000)
        items = list(items)
        tweets = [t.full_text for t in items]
        for index, tweet in enumerate(tweets):
            tags = self.hashtags_from_tweet(tweet)
            for tag in tags:
                if tag not in self.tagignore:
                    if tag in self.hashtags:
                        self.hashtags[tag] = self.hashtags[tag] + 1
                    else:
                        self.hashtags[tag] = 1
        
    def get_hashtags(self):
        return self.hashtags
    
    def hashtags_from_tweet(self, tweet):
        words = tweet.split()
        tags = ["#"+re.sub(r'\W+','', word) for word in words if "#" in word]
        return tags