"""
Provides writing and reading functionality to transform data to and back from CSV.
"""
from pyspark.sql import Row
import csv
from PreProcessTweets import PreProcessTweets

class TweetDataIO():
        
    def __init__(self, filename, spark, context):
        self.filename = filename
        self.spark = spark
        self.context = context
    
    def write(self, tweets=None, label=0, append=True):
        """
        Write the important information of the tweets.
        """
        
        rows = []
        for index, status in enumerate(tweets):
            
            location = status.user.location
            time = status.created_at
            tweet_id = status.id
            handle = status.user.screen_name
            text = status.full_text.replace(";","")
            text = " ".join(text.split())
            tags = PreProcessTweets.get_tags(status.full_text)
            
            # Check if delimiter not broken
            if ";" in location or ";" in handle:
                print("Warning: illegal delimiter ';' in name or location.")
                continue
            
            if "\n" in location or "\n" in handle:
                print("Warning: illegal newline in name or location.")
                continue

            rows.append([tweet_id, text, location, handle, time, label, tags])

        # Append them into a CSV file
        with open(self.filename, ("a" if append else "w"), encoding="utf-8", newline='') as f:
            w = csv.writer(f, delimiter=";")
            for row in rows:
                w.writerow(row)
                
    
    def read(self):
        
        # Load the data with spark and perform PreProcessing    
        lines = self.context.textFile(self.filename)
        parts = lines.map(lambda line: line.split(";"))
        items = parts.map(lambda m: Row(
                                        tweet_id=int(m[0]),
                                        text=m[1],
                                        location=m[2],
                                        user=m[3],
                                        time=m[4],
                                        label=int(m[5]),
                                        tags=m[6]
                                       ))

        try:
            ddf = self.spark.createDataFrame(items)
            return ddf
        except:
            print("There was some sort of error with pyspark. Running this code again can sometimes fix it...")