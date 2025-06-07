from pyspark.sql.functions import col

class Politics:
    def __init__(self):
        pass
    
    def most_upvoted_political_submissions(self):
        pass
    
    def most_controversial_political_submission(self):
        pass
    
    def author_with_most_political_submissions(self):
        pass
    
    def subreddit_with_most_political_discussion(self):
        pass
    
    def political_submissions_with_source_link(self):
        pass
    
    def awarded_political_submissions(self):
        pass
    
class Reddit_Politics(Politics):
    def __init__(self, df_orginial):
        """
            filtering out just political tags submissions
        """
        self.df_politico = df_orginial.filter(col('tags')=='Politics')

    
    
    def most_upvoted_political_submissions(self):
        self.df_most_voted_post_in_politics = self.df_politico.filter(col('ups') > 100);
        
        self.query = self.df_most_voted_post_in_politics.writeStream.outputMode('append').format('console').option("truncate", False).start()
        
        self.query.awaitTermination()