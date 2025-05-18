from pyspark.sql import SparkSession
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
        query = df_query.writeStream.outputMode("append").format("console").option("truncate", False).start()
        time.sleep(10)
        query.stop()
        
        
def data_consumer():
    obj_social_media_feed = Read_Social_Media_Feed("localhost:9092", "social_meida_feed")
    obj_social_media_feed.read_data()