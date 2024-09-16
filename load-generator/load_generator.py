from kafka import KafkaProducer
import csv
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

with open('data.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        message = json.dumps(row).encode('utf-8')
        producer.send('topicA', message)
        print(f"Sent: {message}")
        time.sleep(1)  # Throttle the messages

producer.flush()
producer.close()