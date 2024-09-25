from confluent_kafka import Consumer, KafkaError, KafkaException

# Define Kafka consumer configuration
conf = {
    'bootstrap.servers': 'kafka:9092',  # Kafka service defined in docker-compose.yml
    'group.id': 'my-consumer-group',     # Consumer group ID
    'auto.offset.reset': 'earliest'      # Start consuming from the earliest message if no offset is present
}

# Create Kafka consumer instance
consumer = Consumer(conf)

# Subscribe to a Kafka topic
topic = 'my-topic'  # Replace 'your_topic' with the actual Kafka topic name you're using
consumer.subscribe([topic])

def consume_messages():
    print(f"Subscribed to Kafka topic: {topic}")
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
                print(f"Received message: {msg.value().decode('utf-8')} from topic: {msg.topic()} partition: {msg.partition()}")
    except KeyboardInterrupt:
        print("Consumer interrupted")
    finally:
        # Clean up and close the consumer on exit
        consumer.close()

if __name__ == "__main__":
    consume_messages()