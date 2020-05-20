import tweepy
import csv
import re
import string
import numpy as np
from PreProcessTweets import PreProcessTweets

class LivePredictionStream(tweepy.StreamListener):

    def __init__(self, 
                 file_name, 
                 predictor,
                 num_iter=10, 
                 verbose=0,
                 tagignore=[]):
        
        super().__init__()
        self.file_name = file_name
        self.predictor = predictor
        self.num_ter = num_iter
        self.counter = 1
        self.verbose = verbose
        self.tagignore = tagignore

    def on_status(self, status):
                        
        if not hasattr(status, "retweeted_status"):
            
            with open(self.file_name, "a", encoding='utf-8', newline='') as f:
                                
                text = status.extended_tweet["full_text"] if hasattr(status, "extended_tweet") else status.text
                text = text.replace(",", "")
                text = text.replace(";", "")
                time = status.created_at
                user = status.user.screen_name
                                
                # Preprocess
                text = re.sub(r'\s?http\S+', "", text)
                text = [word for word in text.split() if word not in self.tagignore]
                text = " ".join(text)
                show_text = text
                text = text.translate(str.maketrans('', '', string.punctuation))

                
                if self.verbose > 0:
                    p = self.predictor.model.predict_proba([text])
                    label = np.argmax(p)
                    label_text = ("denial" if label == 0 else "normal")
                    print(f"({self.counter}) {label_text} ({p[0][label]:.2f}): {show_text}")
                    
                location = status.user.location
                coordinates = None
                if status.place is not None:
                    coordinates = status.place.bounding_box.coordinates

                tags = PreProcessTweets.get_tags(text, True)
                
                writer = csv.writer(f, delimiter=";")
                writer.writerow([text, tags, user, time, location, coordinates])
                
                # Increment counter
                self.counter += 1
                
                # End streaming by raising exception
                if self.counter > self.num_ter:
                    raise Exception("Max iterations reached!")
                
    def on_error(self, status_code):
        print("Error: ", status_code)
        return False
    