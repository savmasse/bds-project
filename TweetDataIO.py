"""
Provides writing and reading functionality to transform data to and back from CSV.
"""
from pyspark.sql import Row
import csv

class TweetDataIO():
        
    def __init__(self, filename, spark, context):
        self.filename = filename
        self.spark = spark
        self.context = context
    
    def write(self, tweets=None, label=0, append=True):
        """
        Write the important information of the tweets
        """
        
        rows = []
        for status in tweets:
            location = status.user.location.replace(";","")
            time = status.created_at
            tweet_id = status.id
            handle = status.user.screen_name.replace(";","")
            text = status.full_text.replace(";","") # Remove the CSV delimiter
            text = " ".join(text.split())

            rows.append([tweet_id, text, location, handle, time, label])

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
                                        label=int(m[5])
                                       ))

        try:
            ddf = self.spark.createDataFrame(items, verifySchema=True)
            return ddf
        except:
            print("There was some sort of error with pyspark. Running this code again can sometimes fix it...")