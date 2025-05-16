from producer.producer import producer
from data_processing.data_consumer import data_consumer
import os

if __name__ == '__main__':
    producer()
    data_consumer()