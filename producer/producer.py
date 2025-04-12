#!/usr/bin/env python

from random import choice
from confluent_kafka import Producer
from faker import Faker
import json, time

if __name__ == '__main__':
    
    config = {
        "bootstrap.servers" : 'localhost:9092',
        "acks" : 'all'
    }
    
    producer = Producer(config)
    fake = Faker()
    
    def delivery_callback(err, mssg):
        
        if err:
            print('Error: Message failed delivery: {}'.format(err))
        else:
            print('produced event to topic {topic} : key={key} value:{value}'.format(topic=mssg.topic(), key=mssg.key().decode('utf-8'),value=mssg.value().decode('utf-8')))
            
        
    topic = 'social_meida_feed'
    
    try:
        while True:
            message_dict = {
            "id": fake.uuid4(),
            "username": fake.user_name(),
            "timestamp": fake.iso8601(),
            "content": fake.text(max_nb_chars=140),
            "likes": fake.random_int(min=0, max=1000),
            "shares": fake.random_int(min=0, max=500),
            }
            
            message = json.dumps(message_dict)
            
            producer.produce(topic, key=str(message_dict["id"]), value=message, callback=delivery_callback)
            
            producer.poll(0)
            
            print(f"Produced message: {message}")
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("Stopping producer... Flushing pending messages.")
    finally:       
        producer.flush()
    