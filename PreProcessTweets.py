import re
from nltk.corpus import stopwords

"""
Class for the preprocessing of tweets; involves removing hyperlinks and stopwords.
"""
class PreProcessTweets():
    
    def __init__(self, 
                 tweets,
                 remove_tags=False, 
                 remove_stopwords=False, 
                 remove_urls=False,
                 remove_mentions=False,
                 remove_punctuation=False):

        self.tweets = tweets
        self.remove_tags = remove_tags
        self.remove_stopwords = remove_stopwords
        self.remove_urls = remove_urls
        self.remove_mentions = remove_mentions
        self.remove_punctuation = remove_punctuation
        
    def _remove_urls(self):
        """ Remove all urls from the tweet text. """
        self.tweets = [re.sub(r'\s?http\S+', "", t) for t in self.tweets]
    
    def _remove_stopwords(self):
        """ Remove English stopwords from the text. """
        sw = set(stopwords.words("english")) 
        self.tweets = [" ".join([word for word in c.split() if word not in sw]) for c in self.tweets]

    def _remove_hashtag(self, tag=None):
        """ Remove a specific hashtag. If no tag specified, remove all tags."""
        for index, tweet in enumerate(self.tweets):
            words = tweet.split()
            no_tags = [word for word in words if "#" not in word]
            self.tweets[index] = " ".join(no_tags)
            
    def _remove_mentions(self):
        """ Remove all mentions (@user). """
        self.tweets = [re.sub(r'\s?@\S+', "", t) for t in self.tweets]
    
    def _remove_punctuation(self):
        """ Punctuation affects words: eg. 'however' is not the same word as 'however,'"""
        pass
    
    def preprocess(self):
        """ Perform the requested steps of the preprocessing. """
        
        if self.remove_tags:
            self._remove_hashtag()
            
        if self.remove_stopwords:
            self._remove_stopwords()
            
        if self.remove_urls:
            self._remove_urls()
            
        if self.remove_mentions:
            self._remove_mentions()
        
        if self.remove_punctuation:
            self._remove_punctuation()
            
        return self.tweets
