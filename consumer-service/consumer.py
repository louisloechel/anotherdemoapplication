from confluent_kafka import Consumer, KafkaError, KafkaException # type: ignore
from prometheus_client import start_http_server, Counter, Gauge, Info # type: ignore
import json
import os
import time
import numpy as np
from statsmodels.tsa.holtwinters import SimpleExpSmoothing
from collections import defaultdict

# Prometheus counter for tracking the number of messages
MESSAGE_COUNTER = Counter('kafka_consumer_messages_total', 'Total number of messages consumed')
PREDICTED_PULSE_GAUGE = Gauge('kafka_consumer_predicted_pulse', 'Predicted next pulse', ['userid', 'topic', 'icd10'])
PREDICTED_BPS_GAUGE = Gauge('kafka_consumer_predicted_bps', 'Predicted next BPS', ['userid', 'topic', 'icd10'])
PREDICTED_SHOCK_GAUGE = Gauge('kafka_consumer_predicted_shock', 'Predicted next shock index', ['userid', 'topic', 'icd10'])
SHOCK_GAUGE = Gauge('kafka_consumer_shock', 'Shock index', ['userid', 'topic', 'icd10'])
ICD10_LAST_SEEN = Gauge('kafka_consumer_icd10_last_seen', 'Latest icd10 code seen per userid and topic', ['userid', 'topic', 'icd10'])
PROCESSING_LATENCY = Gauge('kafka_consumer_processing_latency_ms', 'Processing latency in milliseconds', ['userid', 'topic', 'icd10'])

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

#-----------------------------------------
#
#       begin Prediction Use Case

# Historical data for prediction
pulse_history = defaultdict(list)
pulse_history_prink = defaultdict(list)
bps_history = defaultdict(list)
bps_history_prink = defaultdict(list)

# Single models for all patients
pulse_model = None
bps_model = None

# Threshold to start training models (minimum data points)
TRAINING_THRESHOLD = 15
# Window size for training models
WINDOW_SIZE = 3

def train_model(data):
    # Trains an Exponential Smoothing model on the provided data.
    if len(data) > WINDOW_SIZE:
        data = data[-WINDOW_SIZE:]

    model = SimpleExpSmoothing(data).fit()
    return model

def fit_model(model, data):
    # Fits an existing Exponential Smoothing model on the provided data.
    if len(data) > WINDOW_SIZE:
        data = data[-WINDOW_SIZE:]

    model = SimpleExpSmoothing(data).fit()
    return model

def predict_next_value(model):
    # Predict the next value using the trained model.
    y_pred = model.forecast(1)[0]
    return y_pred

def train_and_predict(message, topic):
    # Processes each incoming message.
    print(f"Processing message: {message}")
    global pulse_history, bps_history, pulse_history_prink, bps_history_prink, pulse_model, bps_model

    # Parse the message
    try:
        pulse = message['pulse']
        bps = message['bps']
        userid = message['userid']
        icd10 = message['icd10']

        # Determine the correct histories based on the topic
        if topic == 'prink-topic':
            pulse = process_value(pulse)
            bps = process_value(bps)
            current_pulse_history = pulse_history_prink
            current_bps_history = bps_history_prink
        else:
            current_pulse_history = pulse_history
            current_bps_history = bps_history

        # Update histories
        if pulse is not None:
            current_pulse_history[userid].append(int(pulse))
        if bps is not None:
            current_bps_history[userid].append(int(bps))

        # Update Shock Index Gauge
        shock_index = int(pulse) / int(bps) if int(bps) != 0 else 0
        SHOCK_GAUGE.labels(userid=userid, topic=topic, icd10=icd10).set(shock_index)
        print(f"Patient {userid} - Shock Index: {shock_index}")

        predicted_pulse = None
        predicted_bps = None

        # Train or fit pulse model if data is sufficient
        all_pulse_data = [pulse for history in current_pulse_history.values() for pulse in history]
        if len(all_pulse_data) >= TRAINING_THRESHOLD:
            if pulse_model:
                pulse_model = fit_model(pulse_model, all_pulse_data)
            else:
                pulse_model = train_model(all_pulse_data)
            predicted_pulse = predict_next_value(pulse_model)
            PREDICTED_PULSE_GAUGE.labels(userid=userid, topic=topic, icd10=icd10).set(predicted_pulse)
            print(f"Patient {userid} - Predicted Pulse: {predicted_pulse}")

        # Train or fit BPS model if data is sufficient
        all_bps_data = [bps for history in current_bps_history.values() for bps in history]
        if len(all_bps_data) >= TRAINING_THRESHOLD:
            if bps_model:
                bps_model = fit_model(bps_model, all_bps_data)
            else:
                bps_model = train_model(all_bps_data)
            predicted_bps = predict_next_value(bps_model)
            PREDICTED_BPS_GAUGE.labels(userid=userid, topic=topic, icd10=icd10).set(predicted_bps)
            print(f"Patient {userid} - Predicted BPS: {predicted_bps}")

        # Predict shock index
        if predicted_pulse is not None and predicted_bps is not None:
            shock_index = predicted_pulse / predicted_bps if predicted_bps != 0 else 0
            PREDICTED_SHOCK_GAUGE.labels(userid=userid, topic=topic, icd10=icd10).set(shock_index)
            print(f"Patient {userid} - Predicted Shock Index: {shock_index}")

    except Exception as e:
        print(f"Error processing message: {e}")

#       end Prediction Use Case
#
#-----------------------------------------

def process_value(value):
    # if value is empty string, skip
    if value == '':
        return None
    elif value[0] == '(':
        # value is tuple, (123,456), rm '()', split by ',' and calculate rounded avg
        value = value[1:-1]
        value_avg = (float(value.split(',')[0]) + float(value.split(',')[1])) / 2
        value = round(value_avg, 0)
    return value

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
                time_now = time.time()
                msg_content = msg.value()
                print(f"Received message: {msg_content}")
                message = json.loads(msg_content.decode('utf-8'))

                print(f"Received message: message={message} from topic: {msg.topic()}")
                # Export specific values from the message to Prometheus metrics
                # resp, bps, pulse, temp
                if 'userid' in message:
                    userid = message['userid']
                    icd10 = message['icd10']
                    topic = msg.topic()
                    
                    # Set the processing latency for this message
                    PROCESSING_LATENCY.labels(userid=userid, topic=topic, icd10=icd10).set(time.time() - time_now)

                    # Set/update the icd10_last_seen Gauge for Prometheus
                    ICD10_LAST_SEEN.labels(userid=userid, topic=topic, icd10=icd10).set(1)

                    for key, value in message.items():
                        # Check if the gauge for this key exists with a 'userid' label
                        if key not in gauges:
                            # Define the gauge with 'userid' as a label
                            gauges[key] = Gauge(f'kafka_consumer_{key}', f'Kafka consumer {key} value', ['userid', 'topic', 'icd10'])
                        
                        # Process the value, e.g., if it’s a tuple in string format "(123,456)"
                        if topic == 'prink-topic':# Use the function to process the value
                            processed_value = process_value(value)
                            if processed_value is not None:
                                value = processed_value

                        # Convert the value to a numeric type if needed, e.g., int or float
                        try:
                            value = float(value)
                        except ValueError:
                            continue  # or handle the error as appropriate
                        
                        print(f"(key, value) = ({key}, {value})")
                        
                        # Set the gauge with the specific 'userid' label value
                        gauges[key].labels(userid=userid, topic=topic, icd10=icd10).set(value)

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

                    ## Prediction use case
                    # Expose Shock Index Gauge
                    if 'pulse' in message and 'bps' in message:
                        pulse = message['pulse']
                        bps = message['bps']
                        if topic == 'prink-topic':
                            pulse = process_value(pulse)
                            bps = process_value(bps)
                        shock_index = int(pulse) / int(bps) if int(bps) != 0 else 0
                        SHOCK_GAUGE.labels(userid=userid, topic=topic, icd10=icd10).set(shock_index)
                        print(f"Patient {userid} - Shock Index: {shock_index}")

                    # Predict next values
                    train_and_predict(message, topic)

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