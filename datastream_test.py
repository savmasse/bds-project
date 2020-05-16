import tweepy
from Authentication import Authentication
from PreProcessTweets import PreProcessTweets
import csv

#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):
    
    # Set a static counter
    counter = 0
    
    def __init__(self, file_name):
        super().__init__()
        self.file_name = file_name

    def on_status(self, status):
                
        if not hasattr(status, "retweeted_status"):
            
            with open(self.file_name, "a", encoding='utf-8', newline='') as f:
                                
                text = status.extended_tweet["full_text"] if hasattr(status, "extended_tweet") else status.text
                text = text.replace(",", "")
                text = text.replace(";", "")
                time = status.created_at
                user = status.user.screen_name
                
                print(f'({MyStreamListener.counter}):\t {text}')
                                
                location = status.user.location
                coordinates = None
                if status.place is not None:
                    coordinates = status.place.bounding_box.coordinates

                tags = PreProcessTweets.get_tags(text, True)
                
                writer = csv.writer(f, delimiter=";")
                writer.writerow([text, tags, user, time, location, coordinates])
                
                # Increment counter
                MyStreamListener.counter += 1
                
    def on_error(self, status_code):
        print(status_code)
        return False
      
        
    
if __name__ == "__main__":
             
    FILE_NAME = "#CoronaHoax.csv"
    api = Authentication(isApp=False).get_api()
    auth = api.auth
    myStreamListener = MyStreamListener(FILE_NAME)
    myStream = tweepy.Stream(auth=auth, listener=myStreamListener)
    
    try:
        print('Start streaming...')
        myStream.filter(languages=['en'], 
                        track=["#CoronaHoax", 
                               "#CovidHoax", 
                               "#coronahoax", 
                               "#covidhoax",
                               "plandemic",
                               "Plandemic",
                               "#scamdemic",
                               "#Scamdemic",
                               "#SCAMDEMIC"])

    except KeyboardInterrupt:
        print("Stopped.")
    
    finally:
        print('Done.')
        myStream.disconnect()
        
    
    # Have a look at the data we just streamed
    import pandas as pd
    
    names = ["text", "user", "time", "location", "coordinates"]
    df = pd.read_csv(FILE_NAME, delimiter=";", header=None, names=names)
    print(df.head())