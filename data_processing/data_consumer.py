from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, BooleanType, MapType, ArrayType
from pyspark.sql.functions import from_json
from spark_session import spark_session
import time

spark = spark_session()

class Message_Reader:
    """
        This class loads the social media feed data from kafka consumer into pyspark application for data processing
    """
    
    def __init__(self, host, topic):
        self.host = host
        self.topic = topic
        
    
    def read_data(self):
        """
            This method is implemented in subclass to read the data from kafka topic
        """
        raise ValueError("Not Implemented")
    
class Read_Social_Media_Feed(Message_Reader):
    """
        This is the actual class where methods are implemented to consume the data from kafka topic
    """
    
    def __init__(self, host, topic):
        self.host = host
        self.topic = topic
    
    def read_data(self):
        df_stream = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.host) \
                .option("subscribe", self.topic) \
                .option("includeHeaders", "true") \
                .option("startingOffsets", "earliest")\
                .load()
        df_stream.printSchema()
        df_query = df_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers")
        submission_struct_schema = StructType([
                        StructField("title", StringType(), nullable=True),
                        StructField("id", StringType(), nullable=False),
                        StructField("content", StringType(), nullable=True),
                        StructField("score", IntegerType(), nullable=True),
                        StructField("likes", IntegerType(), nullable=True),  # Note: This might be null if no votes
                        StructField("ups", IntegerType(), nullable=True),
                        StructField("downs", IntegerType(), nullable=True),
                        StructField("upvote_ratio", FloatType(), nullable=True),
                        StructField("total_comments", IntegerType(), nullable=True),
                        StructField("edited", BooleanType(), nullable=True),  # Could also be TimestampType if edited
                        StructField("is_video", BooleanType(), nullable=True),
                        StructField("is_original_content", BooleanType(), nullable=True),
                        StructField("self_post", BooleanType(), nullable=True),
                        StructField("media", MapType(StringType(), StringType()), nullable=True),  # Could be more complex
                        StructField("media_embed", MapType(StringType(), StringType()), nullable=True),
                        StructField("media_only", BooleanType(), nullable=True),
                        StructField("tags", StringType(), nullable=True),  # Flair text
                        StructField("category", StringType(), nullable=True),
                        StructField("content_category", ArrayType(StringType()), nullable=True),
                        StructField("discussion_type", StringType(), nullable=True),
                        StructField("over_18", BooleanType(), nullable=True),
                        StructField("domain", StringType(), nullable=True),
                        StructField("total_awards", IntegerType(), nullable=True),
                        StructField("awards", ArrayType(
                            StructType([
                                StructField("id", StringType(), nullable=True),
                                StructField("name", StringType(), nullable=True),
                                StructField("count", IntegerType(), nullable=True),
                                # Could include more award fields if needed
                            ])
                        ), nullable=True),
                        StructField("author", StringType(), nullable=True),
                        StructField("author_id", StringType(), nullable=True),
                        StructField("post_date", DoubleType(), nullable=True),  # UTC timestamp
                        StructField("post_url", StringType(), nullable=True),
                        StructField("post_permalink", StringType(), nullable=True),
                        StructField("subreddit_name", StringType(), nullable=True),
                        StructField("subreddit_id", StringType(), nullable=True),
                        StructField("subreddit_subscribers", IntegerType(), nullable=True)
                    ])

        df_json_parsed = df_query.select(from_json("value", submission_struct_schema).alias("value"))

        df_submission_flattened = df_json_parsed.selectExpr(
                                            "value.id",
                                            "value.title",
                                            "value.content",
                                            "value.score",
                                            "value.likes",
                                            "value.ups",
                                            "value.downs",
                                            "value.upvote_ratio",
                                            "value.total_comments",
                                            "value.edited",
                                            "value.is_video",
                                            "value.is_original_content",
                                            "value.self_post",
                                            "value.media",
                                            "value.media_embed",
                                            "value.media_only",
                                            "value.tags",
                                            "value.category",
                                            "value.content_category",
                                            "value.discussion_type",
                                            "value.over_18",
                                            "value.domain",
                                            "value.total_awards",
                                            "value.awards",
                                            "value.author",
                                            "value.author_id",
                                            "value.post_date",
                                            "value.post_url",
                                            "value.post_permalink",
                                            "value.subreddit_name",
                                            "value.subreddit_id",
                                            "value.subreddit_subscribers"
        )

        
        # uncomment df_protbuf_map when using manual way of serializing through protobuf and then manually writing into BQ( without spark-bq-connector )
        # df_protbuf_map = df_query.select(from_json("value", submission_struct_schema).alias("value")).withColumn("value", struct(
        #                                                                                                                 col("value.title").alias("title"),
        #                                                                                                                 col("value.id").alias("id"),
        #                                                                                                                 col("value.content").alias("content"),
        #                                                                                                                 col("value.score").alias("score"),
        #                                                                                                                 col("value.likes").alias("likes"),
        #                                                                                                                 col("value.ups").alias("ups"),
        #                                                                                                                 col("value.downs").alias("downs"),
        #                                                                                                                 col("value.upvote_ratio").alias("upvote_ratio"),
        #                                                                                                                 col("value.total_comments").alias("total_comments"),
        #                                                                                                                 col("value.edited").alias("edited"),
        #                                                                                                                 col("value.is_video").alias("is_video"),
        #                                                                                                                 col("value.is_original_content").alias("is_original_content"),
        #                                                                                                                 col("value.self_post").alias("self_post"),
        #                                                                                                                 col("value.media").alias("media"),
        #                                                                                                                 col("value.media_embed").alias("media_embed"),
        #                                                                                                                 col("value.media_only").alias("media_only"),
        #                                                                                                                 col("value.tags").alias("tags"),
        #                                                                                                                 col("value.category").alias("category"),
        #                                                                                                                 col("value.content_category").alias("content_category"),
        #                                                                                                                 col("value.discussion_type").alias("discussion_type"),
        #                                                                                                                 col("value.over_18").alias("over_18"),
        #                                                                                                                 col("value.domain").alias("domain"),
        #                                                                                                                 col("value.total_awards").alias("total_awards"),
        #                                                                                                                 col("value.awards").alias("awards"),
        #                                                                                                                 col("value.author").alias("author"),
        #                                                                                                                 col("value.author_id").alias("author_id"),
        #                                                                                                                 col("value.post_date").cast("long").alias("post_date"),
        #                                                                                                                 col("value.post_url").alias("post_url"),
        #                                                                                                                 col("value.post_permalink").alias("post_permalink"),
        #                                                                                                                 col("value.subreddit_name").alias("subreddit_name"),
        #                                                                                                                 col("value.subreddit_id").alias("subreddit_id"),
        #                                                                                                                 col("value.subreddit_subscribers").alias("subreddit_subscribers")
        #                                                                                                             ))

        # uncomment below 3 line of code when debuggin is required till spark. comment all stream writing code in main file
        # query = df_submission_flattened.writeStream.outputMode("append").format("console").option("truncate", False).start()
        # # time.sleep(10)
        # query.awaitTermination()
                
        return df_submission_flattened
    
        # return df_protbuf_map
        
        
def data_consumer():
    obj_social_media_feed = Read_Social_Media_Feed("localhost:9092", "social_meida_feed")
    return obj_social_media_feed.read_data()