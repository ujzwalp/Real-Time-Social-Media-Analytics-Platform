#!/usr/bin/env python

from confluent_kafka import Producer
from producer.fetch_subreddit_india import Get_India_Feed
from producer.fetch_subreddit_PoliticalDiscussion import Get_PoliticalDiscussion_Feed
import json, time

class Producer_Client:
    def batch_producer(self):
        raise ValueError("Batch producer should be Implemented in sub-class.")
    
    def livestream_producer(self, mssg):
        raise ValueError("Batch producer should be Implemented in sub-class.")
    
class Producer_Client_Application(Producer_Client):
    def __init__(self):
        config = {
            "bootstrap.servers" : 'localhost:9092',
            "acks" : 'all'
        }
        
        self.producer = Producer(config)
        self.topic = 'social_meida_feed'
        
        def delivery_callback(err, mssg):
            if err:
                print('Error: Message delivery falied : {}'.format(err))
            else:
                print('produced event to topic {topic} : key={key} value:{value}'.format(topic=mssg.topic(), key=mssg.key().decode('utf-8'),value=mssg.value().decode('utf-8')))
                
        self.delivery_callback = delivery_callback

    def batch_producer(self):             
        r_india_messages_hot = Get_India_Feed().get_hot_submissions()
        r_india_messages_top = Get_India_Feed().get_top_submissions()
        
        r_political_discussion_messages_hot = Get_PoliticalDiscussion_Feed().get_hot_submissions()
        r_political_discussion_messages_top = Get_PoliticalDiscussion_Feed().get_top_submissions()
        
        try:
            for message in r_india_messages_hot:
                
                message_key = json.loads(message)["subreddit_id"]
                
                self.producer.produce(self.topic, key=str(message_key), value=message, callback=self.delivery_callback)
                
                self.producer.poll(0)
                
                print(f"Produced message for hot submission in r\india: {message}")
                time.sleep(1)
                
            for message in r_india_messages_top:
                
                message_key = json.loads(message)["subreddit_id"]
                
                self.producer.produce(self.topic, key=str(message_key), value=message, callback=self.delivery_callback)
                
                self.producer.poll(0)
                
                print(f"Produced message for top submission in r\india: {message}")
                time.sleep(1)
                
            for message in r_political_discussion_messages_hot:
                
                message_key = json.loads(message)["subreddit_id"]
                
                self.producer.produce(self.topic, key=str(message_key), value=message, callback=self.delivery_callback)
                
                self.producer.poll(0)
                
                print(f"Produced message for hot submission in r\PoliticalDiscussion: {message}")
                time.sleep(1)
                
            for message in r_political_discussion_messages_top:
                
                message_key = json.loads(message)["subreddit_id"]
                
                self.producer.produce(self.topic, key=str(message_key), value=message, callback=self.delivery_callback)
                
                self.producer.poll(0)
                
                print(f"Produced message for top submission in r\PoliticalDiscussion: {message}")
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("Stopping producer")
            

    def livestream_producer(self, message):
        try:
            message_key = json.loads(message)["subreddit_id"]
            
            self.producer.produce(self.topic, key=str(message_key), value=message, callback=self.delivery_callback)
            time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping producer")