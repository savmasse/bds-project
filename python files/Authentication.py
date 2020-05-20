import tweepy

class Authentication():

    # Standard values for the keys
    consumer_key = "H2oOuvgoFBQ4PA1K9Yd8CqdM6"        
    consumer_secret = "zSa6ulXVdNAl1Xk6TMSw48nVXIm88suBF06JzmT5XNSG2AIBxH"
    access_token = "1245776529768034306-YDUC9vTttvxvyhDhRVxGjfbt01p3bd"          
    access_token_secret = "7cDHZiGpHSD2Y8Fe6RdRIpe75WephsSmfU6woDhHlD5BX"
    
    def __init__(self, 
                 isApp=True,
                 consumer_key=None, 
                 consumer_secret=None, 
                 access_token=None,
                 access_token_secret=None):

        if consumer_key is None:
            consumer_key = Authentication.consumer_key
        if consumer_secret is None:
            consumer_secret = Authentication.consumer_secret
        if access_token is None:
            access_token = Authentication.access_token
        if access_token_secret is None:
            access_token_secret = Authentication.access_token_secret
            
        # App has higher allowed download rate, but cannot be used for streaming
        if isApp:
            auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
        else:
            auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)

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