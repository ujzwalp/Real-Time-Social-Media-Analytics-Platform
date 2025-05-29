#!/usr/bin/env python

from confluent_kafka import Producer
from producer.fetch_subreddit_india import Get_India_Feed
from producer.fetch_subreddit_PoliticalDiscussion import Get_PoliticalDiscussion_Feed
import json, time

def producer():
    
    config = {
        "bootstrap.servers" : 'localhost:9092',
        "acks" : 'all'
    }
    
    producer = Producer(config)
    
    def delivery_callback(err, mssg):
        
        if err:
            print('Error: Message failed delivery: {}'.format(err))
        else:
            print('produced event to topic {topic} : key={key} value:{value}'.format(topic=mssg.topic(), key=mssg.key().decode('utf-8'),value=mssg.value().decode('utf-8')))
            
        
    topic = 'social_meida_feed'
    
    r_india_messages_hot = Get_India_Feed().get_hot_submissions()
    r_india_messages_top = Get_India_Feed().get_top_submissions()
    
    r_political_discussion_messages_hot = Get_PoliticalDiscussion_Feed().get_hot_submissions()
    r_political_discussion_messages_top = Get_PoliticalDiscussion_Feed().get_top_submissions()
    
    try:
        for message in r_india_messages_hot:
            
            message_key = json.loads(message)["subreddit_id"]
            
            producer.produce(topic, key=str(message_key), value=message, callback=delivery_callback)
            
            producer.poll(0)
            
            print(f"Produced message for hot submission in r\india: {message}")
            time.sleep(1)
            
        for message in r_india_messages_top:
            
            message_key = json.loads(message)["subreddit_id"]
            
            producer.produce(topic, key=str(message_key), value=message, callback=delivery_callback)
            
            producer.poll(0)
            
            print(f"Produced message for top submission in r\india: {message}")
            time.sleep(1)
            
        for message in r_political_discussion_messages_hot:
            
            message_key = json.loads(message)["subreddit_id"]
            
            producer.produce(topic, key=str(message_key), value=message, callback=delivery_callback)
            
            producer.poll(0)
            
            print(f"Produced message for hot submission in r\PoliticalDiscussion: {message}")
            time.sleep(1)
            
        for message in r_political_discussion_messages_top:
            
            message_key = json.loads(message)["subreddit_id"]
            
            producer.produce(topic, key=str(message_key), value=message, callback=delivery_callback)
            
            producer.poll(0)
            
            print(f"Produced message for top submission in r\PoliticalDiscussion: {message}")
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("Stopping producer... Flushing pending messages.")
    finally:       
        producer.flush()