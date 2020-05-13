import tweepy

class Authentication():

    # Standard values for the keys
    consumer_key = "H2oOuvgoFBQ4PA1K9Yd8CqdM6"        
    consumer_secret = "zSa6ulXVdNAl1Xk6TMSw48nVXIm88suBF06JzmT5XNSG2AIBxH"

    def __init__(self, consumer_key=None, consumer_secret=None):

        if consumer_key is None:
            consumer_key = Authentication.consumer_key
        if consumer_secret is None:
            consumer_secret = Authentication.consumer_secret

        auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)

        self.api = tweepy.API(
                        auth,
                        wait_on_rate_limit=True,
                        wait_on_rate_limit_notify=True
                        )
        
    def get_api(self):
        if not self.api:
            print("Can't authenticate. Check if credentials are correct.")
        else:
            return self.api