from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time

spark = SparkSession.builder.appName("Social_Media_Feed_Analysis_App").config("spark.master", "local[*]") .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5").getOrCreate()

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
        
        submission_struct_schema = reddit_schema = StructType([
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
                        StructField("post_date", TimestampType(), nullable=True),  # UTC timestamp
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

        query = df_submission_flattened.writeStream.outputMode("append").format("console").option("truncate", False).start()
        time.sleep(10)
        query.awaitTermination()
        
        
def data_consumer():
    obj_social_media_feed = Read_Social_Media_Feed("localhost:9092", "social_meida_feed")
    obj_social_media_feed.read_data()