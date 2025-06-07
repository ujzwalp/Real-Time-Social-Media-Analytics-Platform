import praw, os, json
from producer.producer import Producer_Client_Application
from dotenv import load_dotenv
load_dotenv()

class LiveStream:
    def __init__(self):
        self._user_agent = "script:producer_reddit_api:v1.0 (by /u/ujzwalp1710)"
        self._client_id = os.getenv("client_id")
        self._client_secret = os.getenv("client_secret")
    
    def livestream_new_submissions(self):
        raise ValueError("No Implemented.")
    
class LiveStream_Submissions(LiveStream):
    def __init__(self):
        super().__init__()
        
        self.reddit = praw.Reddit(
            client_id = self._client_id,
            client_secret = self._client_secret,
            refresh_token = os.getenv("refresh_token"),
            user_agent = self._user_agent
        )
        
        print("Script granted access to the api with the following privilege: ",self.reddit.auth.scopes())  
        
        self.subreddit = self.reddit.subreddit("India+PoliticalDiscussion")
        
        print("########## Ready to Simulate Livestreaming of the new reddit submissions ##########")
        
    def livestream_new_submissions(self):
        for submission in self.subreddit.stream.submissions(pause_after=0):
            if submission is None:
                continue
            
            message_dict = {
                "id": submission.id,
                "title": submission.title,
                "content": submission.selftext,
                "score": submission.score,
                "likes": submission.likes,
                "ups": submission.ups,
                "downs": submission.downs,
                "upvote_ratio": submission.upvote_ratio,
                "total_comments": submission.num_comments,
                "edited": submission.edited,
                "is_video": submission.is_video,
                "is_original_content": submission.is_original_content,
                "self_post": submission.is_self,
                "media": submission.media,
                "media_embed": submission.media_embed,
                "media_only": submission.media_only,
                "tags":submission.link_flair_text,
                "category": submission.category,
                "content_category": submission.content_categories,
                "discussion_type": submission.discussion_type,
                "over_18": submission.over_18,
                "domain": submission.domain,
                "total_awards": submission.total_awards_received,
                "awards": submission.all_awardings,
                "author": submission.author.name if submission.author else "[deleted]",
                "author_id": getattr(submission.author, "id", "t2_NA"),
                "post_date": submission.created_utc,
                "post_url": submission.url,
                "post_premalink": submission.permalink,
                "subreddit_name": submission.subreddit_name_prefixed,
                "subreddit_id": submission.subreddit_id,
                "subreddit_subscribers": submission.subreddit_subscribers,
            }
            
            message = json.dumps(message_dict)
            
            Producer_Client_Application().livestream_producer(message)