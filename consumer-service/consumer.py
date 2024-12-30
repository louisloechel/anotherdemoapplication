from confluent_kafka import Consumer, KafkaError, KafkaException # type: ignore
from prometheus_client import start_http_server, Counter, Gauge # type: ignore
import json
import os
import time

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
raw = 'raw-topic'  
processed = 'processed-topic'
prink = 'prink-topic'

topics = [processed, prink]
consumer.subscribe(topics)

# Prometheus metrics
gauges = {}

# Wait for the topic to be available
sleeptime = 10
print(f"Waiting {sleeptime}s for Kafka topics {topics} to be available...", flush=True)
time.sleep(sleeptime)
print("Starting consumer...", flush=True)



def consume_messages():
    print(f"Subscribing to Kafka topics: {topics}", flush=True)
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for a new message from the topic

            if msg is None:
                # No new message available, continue polling
                print("No new message available, continuing polling...")
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
                msg_content = msg.value()
                print(f"Received message: {msg_content}")
                message = json.loads(msg_content.decode('utf-8'))

                print(f"Received message: message={message} from topic: {msg.topic()}")
                # Export specific values from the message to Prometheus metrics
                # resp, bps, pulse, temp
                if 'userid' in message:
                    userid = message['userid']
                    waveformlabel = message['waveformlabel']
                    for key, value in message.items():
                        topic = msg.topic()
                        # Check if the gauge for this key exists with a 'userid' label
                        if key not in gauges:
                            # Define the gauge with 'userid' as a label
                            gauges[key] = Gauge(f'kafka_consumer_{key}', f'Kafka consumer {key} value', ['userid', 'topic', 'waveformlabel'])
                        
                        # Process the value, e.g., if it’s a tuple in string format "(123,456)"
                        if topic == 'prink-topic':
                            # if value is empty string, skip
                            if value == '':
                                continue
                            elif value[0] == '(':
                                # value is tuple, (123,456), rm (), split by , and take first element
                                value = value[1:-1]
                                value_avg = (float(value.split(',')[0]) + float(value.split(',')[1])) / 2
                                value = round(value_avg, 0)
                        
                        # Convert the value to a numeric type if needed, e.g., int or float
                        try:
                            value = float(value)
                        except ValueError:
                            continue  # or handle the error as appropriate
                        
                        print(f"(key, value) = ({key}, {value})")
                        
                        # Set the gauge with the specific 'userid' label value
                        gauges[key].labels(userid=userid, topic=topic, waveformlabel=waveformlabel).set(value)

                    ## QA use case
                    # check if the 'correct_bed_registration' gauge exists, if not, create it
                    if 'correct_bed_registration' not in gauges:
                        gauges['correct_bed_registration'] = Gauge('kafka_consumer_correct_bed_registration', 'Kafka consumer correct bed registration', ['userid', 'topic'])

                    # Set the bool gauge for correct bed registration to true, if:
                    # - username is not "Unknown"
                    # - recordis has length 7 and starts with "03"
                    username = message['username']
                    uid = str(message['userid'])
                    if username != "Unknown" and len(uid) == 7 and uid.startswith("03"):
                        gauges['correct_bed_registration'].labels(userid=userid, topic=topic).set(1)
                    else:
                        gauges['correct_bed_registration'].labels(userid=userid, topic=topic).set(0)

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
            consumer.subscribe(topics)
            consume_messages()
        except KafkaException as e:
            print(f"KafkaException: {e}", flush=True)
            time.sleep(5)  # Wait before retrying
        except RuntimeError as e:
            print(f"RuntimeError: {e}", flush=True)
            time.sleep(5)  # Wait before retrying