from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE', 'http://hadoop:9870')
HDFS_USER = 'root'

consumer = KafkaConsumer(
    'topicB',
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='data-sink-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

hdfs_client = InsecureClient(HDFS_NAMENODE, user=HDFS_USER)

for message in consumer:
    data = message.value
    print(f"Received data: {data}")
    # Write data to HDFS
    with hdfs_client.write('/kafka-data/data.json', append=True, encoding='utf-8') as writer:
        writer.write(json.dumps(data) + '\n')

consumer.close()