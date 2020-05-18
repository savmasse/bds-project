import tweepy
from HashtagFinder import HashtagFinder
from PreProcessTweets import PreProcessTweets
import pandas as pd


class DataMiner:
    
    def __init__(self, api, starting_hashtag, location_radius, language, tagignore = [], num_tweets=500):
        self.api = api
        self.location_radius = location_radius
        self.language = language
        self.denial_tweets = []
        self.ids = []
        self.starting_hashtag = starting_hashtag
        self.finder = HashtagFinder(api, starting_hashtag, location_radius, language, tagignore)
        self.tags_panda = pd.DataFrame(columns=['Author', 'Location', 'Tags'])
        self.num_tweets = num_tweets
        
    def _collect_tweets(self):
        # find relevant hashtags to search for
        self.finder.collect_tags()
        denial_tags = self.finder.get_hashtags()
        denial_tags = {k: v for k, v in sorted(denial_tags.items(), key=lambda item: item[1]) if v >= 15}
        
        for k, v in denial_tags.items():
            print("Processing tag: " + k)
            search_term = k + " -filter:retweets"
            items = tweepy.Cursor(self.api.search,
                                q = search_term,
                                geocode=self.location_radius,
                                count=100,
                                lang=self.language,
                                include_rts=False,
                                tweet_mode="extended").items(self.num_tweets)
            items = list(items)
            for item in items:
                if item not in self.ids:
                    self.ids.append(item.id)
                    self.denial_tweets.append(item)
                    self.tags_panda = self.tags_panda.append({'Author': item.author.name,
                                                            'Location': item.author.location,
                                                            'Tags': PreProcessTweets.get_tags(item.full_text, True)},
                                                            ignore_index=True)

    def mine(self, starting_tag = None):
        if not starting_tag:
            self.starting_hashtag = starting_tag
        self._collect_tweets()
        return self.denial_tweets

    def get_dataframe(self):
        return self.tags_panda

