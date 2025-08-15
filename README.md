# Real-Time-Social-Media-Analytics-Platform

This is for my reference.

## Steps taken so far

1. Created a virtual environment named "analytics-env" with `python3 -m venv analytics-venv`. After creating the virtual environment, run `source analytics-venv/bin/activate` to activate. Run `deactivate` to deactivate after the project is done.

2. Created a requirements text file with `pip freeze > requirements.txt`. If we want to recreate this environment on another system, we can do so by running `pip install -r requirements.txt`.

3. In WSL, cd /mnt/d/kafka_2.13-4.0.0 and run:  rm -rf /tmp/kafka-logs /tmp/kraft-combined-logs

4. Generate kafka cluster UUID: KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

5. format log directories: bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties

6. Start the Kafka Server: bin/kafka-server-start.sh config/server.properties

7. In another terminal, cd /mnt/d/kafka_2.13-4.0.0

8. Create topic using command: bin/kafka-topics.sh --create --topic social_meida_feed --partitions 2 --bootstrap-server localhost:9092

9. cd /mnt/d/'Real-Time Social Media Analytics Platform'
   note: run this command one-time to generate descriptor file for protobuf: protoc --descriptor_set_out=./protobuf/submission_schema.desc --include_imports --proto_path=./protobuf/ ./protobuf/proto_reddit_post.proto

10. Run main script when using spark-bq-connector: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 main.py
    Note: Run main script when using protobuf: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.spark:spark-protobuf_2.12:3.5.5 main.py