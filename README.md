# Real-Time-Social-Media-Analytics-Platform

This is for my reference.

## Steps taken so far

1. Created a virtual environment named "analytics-env" with `python3 -m venv analytics-venv`. After creating the virtual environment, run `source analytics-venv/bin/activate` to activate. Run `deactivate` to deactivate after the project is done.

2. Installed `confluent-kafka`, `kafka-python`, and `faker` in this virtual environment.

3. Created a requirements text file with `pip freeze > requirements.txt`. If we want to recreate this environment on another system, we can do so by running `pip install -r requirements.txt`.

4. In WSL, cd to D:\kafka_2.13-4.0.0

5. Generate kafka cluster UUID: KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

6. format log directories: bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties

7. Start the Kafka Server: bin/kafka-server-start.sh config/server.properties

8. In another terminal, cd to D:\kafka_2.13-4.0.0

9. Create topic using command: bin/kafka-topics.sh --create --topic social_meida_feed --partitions 4 --bootstrap-server localhost:9092

8. Run main script : spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 main.py