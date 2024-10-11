from confluent_kafka import Consumer, KafkaError, KafkaException # type: ignore
from prometheus_client import start_http_server, Counter
import json
import os
import time

print(f"PYTHONUNBUFFERED={os.getenv('PYTHONUNBUFFERED')}", flush=True)

# Prometheus counter for tracking the number of messages
MESSAGE_COUNTER = Counter('kafka_consumer_messages_total', 'Total number of messages consumed')

# Define Kafka consumer configuration
conf = {
    'bootstrap.servers': 'kafka:29092',  # Kafka service defined in docker-compose.yml
    'group.id': 'my-consumer-group',     # Consumer group ID
    'auto.offset.reset': 'earliest',      # Start consuming from the earliest message if no offset is present
}

# Create Kafka consumer instance
consumer = Consumer(conf)

# Subscribe to a Kafka topic
# topic = 'raw-topic'  
topic = 'processed-topic'
consumer.subscribe([topic])

# Wait for the topic to be available
sleeptime = 10
print(f"Waiting {sleeptime}s for Kafka topic {topic} to be available...", flush=True)
time.sleep(sleeptime)
print("Starting consumer...", flush=True)

def consume_messages():
    print(f"Subscribing to Kafka topic: {topic}", flush=True)
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for a new message from the topic

            if msg is None:
                # No new message available, continue polling
                continue

            if msg.error():
                # Handle any errors in message consumption
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, nothing wrong, just move on
                    print(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Properly received a message
                # print(f"Received message: {msg.value().decode('utf-8')} from topic: {msg.topic()} partition: {msg.partition()}")
                message = json.loads(msg.value().decode('utf-8'))
                message_count = message["message_count"]

                print(f"Received message: message_count={message_count} from topic: {msg.topic()} partition: {msg.partition()}")
                
                MESSAGE_COUNTER.inc()  # Increment the Prometheus counter for each message consumed
    except KeyboardInterrupt:
        print("Consumer interrupted")
    finally:
        # Clean up and close the consumer on exit
        consumer.close()

if __name__ == "__main__":
    start_http_server(8000)  # Start the Prometheus metrics server on port 8000
    while True:
        try:
            consumer = Consumer(conf)
            consumer.subscribe([topic])
            consume_messages()
        except KafkaException as e:
            print(f"KafkaException: {e}", flush=True)
            time.sleep(5)  # Wait before retrying
        except RuntimeError as e:
            print(f"RuntimeError: {e}", flush=True)
            time.sleep(5)  # Wait before retrying