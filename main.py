from producer.producer import Producer_Client_Application
from producer.livestream_subreddit_submissions import LiveStream_Submissions
from data_processing.data_consumer import data_consumer
from data_processing.political_analysis import Reddit_Politics

if __name__ == '__main__':
    # Producer_Client_Application().batch_producer()
    # LiveStream_Submissions().livestream_new_submissions()
    df_original = data_consumer()
    Reddit_Politics(df_original).most_upvoted_political_submissions()
    