from pyspark.sql.protobuf.functions import to_protobuf


class Serialization:
    def __init__(self):
        pass
    
    def serialize(self, df_parsed):
        raise ValueError("Implementation not performed.")
    
    
class Protobuf_Serialization(Serialization):
    def __init__(self):
        pass
    
    def serialize(self, df_parsed):
        self.serialized_data = df_parsed \
                            .select(to_protobuf("value", "reddit.post.v1.Submission", "./protobuf/submission_schema.desc").alias("value"))
                            
        self.serialized_data.printSchema()
        
        self.query = self.serialized_data.writeStream.outputMode('append').format('console').option("truncate", False).start()
        
        self.query.awaitTermination()