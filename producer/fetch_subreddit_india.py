import praw, os, re
from dotenv import load_dotenv
load_dotenv()

class Subreddit_india:
    def __init__(self):
        pass
    
    
    def create_reddit_instance(self):
        """
            Implemented in the sub-class.
        """
        
    
        
class Get_India_Feed(Subreddit_india):
    def __init__(self):
        self._user_agent = "script:producer_reddit_api:v1.0 (by /u/ujzwalp1710)"
        self._client_id = os.getenv("client_id")
        self._client_secret = os.getenv("client_secret")
            
    def create_reddit_instance(self):
        reddit = praw.Reddit(
            client_id = self._client_id,
            client_secret = self._client_secret,
            refresh_token = os.getenv("refresh_token"),
            user_agent = self._user_agent
        )
        
        print("Script granted access to the api with the following privilege: ", reddit.auth.scopes())
        
indian_feed_obj = Get_India_Feed()
indian_feed_obj.create_reddit_instance()