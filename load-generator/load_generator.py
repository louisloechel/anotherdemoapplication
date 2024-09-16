from kafka import KafkaProducer
import csv
import json
import time
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
CSV_FILE = 'data.csv'
TOPIC = 'topicA'
SLEEP_INTERVAL = 1  # Time in seconds between messages

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

try:
    while True:
        with open(CSV_FILE, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                producer.send(TOPIC, value=row)
                print(f"Sent: {row}")
                time.sleep(SLEEP_INTERVAL)
except KeyboardInterrupt:
    print("Stopping load generator...")
finally:
    producer.flush()
    producer.close()