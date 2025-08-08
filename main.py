from producer.producer import Producer_Client_Application
from producer.livestream_subreddit_submissions import LiveStream_Submissions
from data_processing.data_consumer import data_consumer
from data_processing.political_analysis import Reddit_Politics
# from bq_sink.serialize_submission import Protobuf_Serialization
from bq_sink.bigquery_ingestion import BQ_Sink_Ingestion
import threading

if __name__ == '__main__':
    # Producer_Client_Application().batch_producer()
    # LiveStream_Submissions().livestream_new_submissions()
    t1 = threading.Thread(target=LiveStream_Submissions().livestream_new_submissions)
    t1.start()
    df_original = data_consumer()
    # Reddit_Politics(df_original).most_upvoted_political_submissions()
    # Protobuf_Serialization().serialize(df_original)
    BQ_Sink_Ingestion(df_original).bq_stream_write_direct()