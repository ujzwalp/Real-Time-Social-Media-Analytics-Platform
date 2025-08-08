from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
load_dotenv()



def spark_session(app_name="Social_Media_Feed_Analysis_App"):
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
        
    spark = SparkSession.builder\
                        .appName(app_name)\
                        .config("spark.master", "local[*]")\
                        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")\
                        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
                        .config("credentialsFile", key_path) \
                        .config("viewsEnabled", "true") \
                        .config("materializationProject", "sturdy-cable-467613-n5") \
                        .config("materializationDataset", "Reddit_Stream") \
                        .getOrCreate()
                        
    return spark