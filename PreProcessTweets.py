import re
import string
from nltk.corpus import stopwords
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

"""
Class for the preprocessing of tweets; involves removing hyperlinks, stopwords etc.
"""
class PreProcessTweets():
    
    def __init__(self, 
                 ddf,
                 remove_tags=False, 
                 remove_stopwords=False, 
                 remove_urls=False,
                 remove_mentions=False,
                 remove_punctuation=False):

        self.ddf = ddf
        self.remove_tags = remove_tags
        self.remove_stopwords = remove_stopwords
        self.remove_urls = remove_urls
        self.remove_mentions = remove_mentions
        self.remove_punctuation = remove_punctuation
        
        
    def _remove_urls(self):
        """ Remove all urls from the tweet text. """
        udf_remove_urls = udf(lambda x: re.sub(r'\s?http\S+', "", x), StringType())
        self.ddf = self.ddf.withColumn("processed_text", udf_remove_urls("processed_text"))
        
        
    def _remove_stopwords(self):
        """ Remove English stopwords from the text. """  
        sw = set(stopwords.words("english"))
        def rm_sw(line):
            return " ".join([word for word in line.split() if word not in sw])
        udf_remove_stopwords = udf(lambda x: rm_sw(x), StringType())
        self.ddf = self.ddf.withColumn("processed_text", udf_remove_stopwords("processed_text"))

        
    def _remove_hashtag(self):
        """ Remove hashtags from the text. """
        def rm_tags(line):
            words = line.split()
            no_tags = [word for word in words if "#" not in word]
            return " ".join(no_tags)
        udf_remove_hashtags = udf(lambda x: rm_tags(x), StringType())
        self.ddf = self.ddf.withColumn("processed_text", udf_remove_hashtags("processed_text"))
    
    
    def _remove_mentions(self):
        """ Remove all mentions (@user). """
        udf_remove_mentions = udf(lambda x: re.sub(r'\s?@\S+', "", x), StringType())
        self.ddf = self.ddf.withColumn("processed_text", udf_remove_mentions("processed_text"))
    

    def _remove_punctuation(self):
        """ Punctuation affects words: eg. 'however' is not the same word as 'however,'"""
        udf_remove_punctuation = udf(lambda x: x.translate(str.maketrans('', '', string.punctuation)), StringType())
        self.ddf = self.ddf.withColumn("processed_text", udf_remove_punctuation("processed_text"))

    
    def _cleanup(self):
        """ Clean up the text by removing all types of excess whitespaces. """
        udf_cleanup = udf(lambda x: " ".join(x.split()), StringType())
        self.ddf = self.ddf.withColumn("processed_text", udf_cleanup("processed_text"))
        
    @staticmethod
    def get_tags(tweet, string=True):
        words = tweet.split()
        if string:
            tags = ""
            for word in words:
                if "#" in word:
                    tags = tags + word + "|"
            if tags != "":
                tags = tags[:-1]
        else:
            tags = [word for word in words if "#" in word]
        return tags
    
    def preprocess(self):
        """ Perform the requested steps of the preprocessing. """
        
        print("Preprocessing...")
        # Create column for the newly processed text
        self.ddf = self.ddf.withColumn("processed_text", F.col("text"))
        
        # Perform the requested preprocessing steps on this new column
        if self.remove_stopwords:
            print(">> Removing stopwords...")
            self._remove_stopwords()
        if self.remove_urls:
            print(">> Removing urls...")
            self._remove_urls()
        if self.remove_tags:
            print(">> Removing hashtags...")
            self._remove_hashtag()
        if self.remove_mentions:
            print(">> Removing user mentions...")
            self._remove_mentions()
        if self.remove_punctuation:
            print(">> Removing punctuation...")
            self._remove_punctuation()
            
        # Clean up the excess whitespaces
        print(">> Removing whitespace...")
        self._cleanup()
        
        print("Finished preprocessing!")
        
        # Return the dataframe with the new columns
        return self.ddf
    
p = PreProcessTweets(None)