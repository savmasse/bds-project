import tweepy
from HashtagFinder import HashtagFinder

class DataMiner():
    
    def __init__(self, api, starting_hashtag, location_radius, language, tagignore = []):
        self.api = api
        self.location_radius = location_radius
        self.language = language
        self.denial_tweets = []
        self.ids = []
        self.finder = HashtagFinder(api, starting_hashtag, location_radius, language, tagignore)
    
    def _collect_tweets(self):
        #find relevant hashtags to search for
        self.finder.collect_tags()
        denial_tags = self.finder.get_hashtags()
        denial_tags = {k: v for k, v in sorted(denial_tags.items(), key=lambda item: item[1]) if v >= 50}
        
        for k, v in denial_tags.items():
            print("Processing tag: " + k)
            search_term = k + " -filter:retweets"
            items = tweepy.Cursor(self.api.search,
                                q = search_term,
                                geocode=self.location_radius,
                                count=100,
                                lang=self.language,
                                include_rts=False,
                                tweet_mode="extended").items(500)
            items = list(items)
            for item in items:
                if item not in self.ids:
                    self.ids.append(item.id)
                    self.denial_tweets.append(item.full_text)
                    
    def mine(self):
        self._collect_tweets()
        return self.denial_tweets