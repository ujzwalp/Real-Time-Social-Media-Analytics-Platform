"""_summary_

    Run this code in a different wsl terminal to perform authorization of your script against your reddit user account.
"""

import praw, time, threading
from flask import Flask, request
import logging, os, re
from dotenv import load_dotenv
load_dotenv()

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
for logger_name in ("praw", "prawcore"):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

class Authentication:
    def __init__(self):
        pass
    
    def OAuth2(self):
        """_summary_

            Implemented in subclass.
        """
        raise ValueError("Not Authorization intiated.")
    
class OAuthRedditAccess(Authentication):
    def __init__(self, client_id, client_secret, user_agent, redirect_url):
        self.user_agent = user_agent
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_url = redirect_url
        
    def _fetch_code(self):
        app = Flask(__name__)
        self.code = None

        @app.route("/")
        def handle_redirect():
            self.code = request.args.get("code")
            return "Authorization complete! You can close this window."
        
        threading.Thread(target=app.run, kwargs={"port": 8080}).start()
    
    
        while not self.code:
            time.sleep(0.1)
        return self.code
    
    def OAuth2(self):
        reddit = praw.Reddit(
                            client_id=self.client_id,
                            client_secret=self.client_secret,
                            redirect_uri=self.redirect_url,
                            user_agent=self.user_agent
        )
        
        _auth_url = reddit.auth.url(
                        scopes=["identity","read"],
                        state="authAccess",
                        duration="permanent"
        )

        print("Authorization url is ",_auth_url)
        
        _short_lived_code = self._fetch_code()
        
        _refresh_token = reddit.auth.authorize(_short_lived_code)
        
        # os.environ.setdefault("refresh_token", _refresh_token)
        
        with open('.env', 'r') as envfile:
            env = envfile.read()
            print(env)
            pattern = re.compile(r'refresh_token=(.+)')
            new_txt = pattern.sub("refresh_token="+'"'+_refresh_token+'"', env)

        with open('.env', 'w') as envFile:
            envFile.write(new_txt)
        
        print("Authorization completed for ",reddit.user.me())
        
        
def OAth2Func():
    user_agent = 1
    redirect_url = "http://localhost:8080"
    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")
    
    OAuth_obj = OAuthRedditAccess(client_id, client_secret, user_agent, redirect_url)
    OAuth_obj.OAuth2()
    
OAth2Func()