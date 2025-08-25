from dotenv import load_dotenv
load_dotenv()

class BigQuery_Ingestion:
    def __init__(self):
        pass
    
    def bq_stream_write(self):
        raise ValueError("Implementation not performed.")
    
    def write_each_stream_batch(self, batch_df, batch_id):
        raise ValueError("Implementation performed in sub-class")
    
    
class BQ_Sink_Ingestion(BigQuery_Ingestion):
    '''
        This class Implements the writing each batch of streaming data into Google cloud bigquery using direct method of spark-bigquery-connector.
        It creates stream for batches and save them in GBQ using batch mode
        
        references: https://github.com/GoogleCloudDataproc/spark-bigquery-connector?tab=readme-ov-file#indirect-write
        examples: https://medium.com/google-cloud/streaming-events-to-bigquery-using-spark-structured-streaming-96cf541de4ed
        
    '''
    
    def __init__(self, df_flattened):
        """
            media and media_embed columns are mapped(converted) to BQ compatible schema.
        """
        self.df_flattened = df_flattened

    
    def bq_stream_write_direct(self):
        """
            creates a stream and uses foreachBatch to direct each batch of stream into BQ
        """
        self.query = self.df_flattened.writeStream\
                                    .foreachBatch(self.write_each_stream_batch)\
                                    .outputMode("append")\
                                    .option("checkpointLocation", "./tmp/bq_checkpoint") \
                                    .option("writeAtleastonce", "true")\
                                    .start()
        
        self.query.awaitTermination()
        
    def write_each_stream_batch(self, batch_df, batch_id):
        """
            uses direct write mode to write a batch into GBQ
            https://medium.com/google-cloud/streaming-events-to-bigquery-using-spark-structured-streaming-96cf541de4ed
        """
        batch_df.write\
            .format("bigquery")\
            .option('writeMethod', "direct")\
            .option("table", "Reddit_Stream.subreddit_stream_submission_analysis")\
            .option("parentProject","sturdy-cable-467613-n5")\
            .mode("append")\
            .save()