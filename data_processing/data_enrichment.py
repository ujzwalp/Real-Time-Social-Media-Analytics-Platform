from pyspark.sql.functions import map_entries, from_unixtime, col, trim, lower
from pyspark.sql.types import TimestampType

class Enrich:
    def __init__(self):
        pass
    
    def clean_data(self, df_flattened):
        raise ValueError("This is implemented in sub-class")
    
    def null_handling(self):
        raise ValueError("Null values are taken care in sub-class")
    
    def text_normalization(self):
        raise ValueError("Text values are normalized in sub-class")
    
    def column_type_mapping(self):
        raise ValueError("media and media_embed are mapped into BQ compatible datatype in sub-class")
    
    def post_date_conversion(self):
        raise ValueError("Date conversion implementation in sub-class")

class Data_Enrichment(Enrich):
    """_summary_

    This class is the sub-class implementation of the abstract methods of the "Enrich" superclass. Implemented methods deals with the flattened dataframe
    to normalize, clean and produce quality data which is further going to be ingested into BG
    
    """
    
    def __init__(self, df_flattened):  
        self.df_flattened = df_flattened
    
    def clean_data(self):
        self.df_flattened = self.df_flattened.dropna('all')
        self.df_flattened = self.df_flattened.dropna(subset=["content"])
        
    def drop_duplicates(self):
        self.df_flattened = self.df_flattened.dropDuplicates()
            
    def null_handling(self):
        self.df_flattened = self.df_flattened\
                                        .fillna(
                                            {
                                                "likes": 0,
                                                "tags": "general",
                                                "category": "undefined",
                                                "discussion_type": "normal",
                                                "post_permalink": "NA",
                                            }
                                        )
    
    def text_normalization(self):
        self.df_flattened = self.df_flattened\
                                    .withColumn("title", trim(lower(col("title"))))\
                                    .withColumn("content", trim(lower(col("content"))))\
                                    .withColumn("tags", trim(lower(col("tags"))))\
    
    def column_type_mapping(self):
        self.df_flattened = self.df_flattened.withColumn("media", map_entries("media")) \
                                    .withColumn("media_embed", map_entries("media_embed"))

    def post_date_conversion(self):
        self.df_flattened = self.df_flattened.withColumn("post_date", from_unixtime(col("post_date")).cast(TimestampType()))
        
    def processed_dataframe(self):
        self.column_type_mapping()
        self.clean_data()
        self.drop_duplicates()
        self.null_handling()
        self.text_normalization()
        self.post_date_conversion()
        
        # uncomment below for data encrichment debugging
        # query = self.df_flattened.writeStream.outputMode("append").format("console").option("truncate", False).start()
        # query.awaitTermination()
        
        return self.df_flattened